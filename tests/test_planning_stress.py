"""
2-worker concurrency stress test for PLANNING stage.
Tests race conditions, locking, and idempotency with parallel execution.
"""

import uuid
import pytest
import threading
import time
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.pipeline_stages import PipelineStage
from core.stage_coordinator import StageCoordinator, StageBeginResult
from core.stage_execution import verify_stage_done


@pytest.fixture
def coordinator():
    return StageCoordinator()


def test_planning_2worker_stress_no_conflicts(coordinator):
    """
    Stress test: 2 workers concurrently processing different builds in PLANNING stage.
    Ensures no lock conflicts and all stages complete successfully.
    """
    db = coordinator.db
    num_builds = 10
    build_ids = []
    
    # Create multiple builds
    for i in range(num_builds):
        build_id = f"stress_plan_{uuid.uuid4().hex[:8]}"
        db.create_build(
            build_id,
            {
                "code_type": "test",
                "description": f"stress test build {i}",
                "request": {
                    "tool_combinations": [{"name": "stack1"}],
                    "factory_review": {"overall_score": 80},
                },
            },
        )
        db.save_checkpoint(build_id, {
            "tool_combinations": [{"name": "stack1"}],
            "factory_review": {"overall_score": 80},
        })
        build_ids.append(build_id)
    
    results = {"success": [], "locked": [], "errors": []}
    lock = threading.Lock()
    
    def worker_process_build(worker_id: int, build_id: str):
        """Worker thread that processes a build through PLANNING stage."""
        try:
            lock_token = f"lock-{worker_id}-{uuid.uuid4().hex[:8]}"
            
            # Attempt to begin stage
            begin_result = coordinator.begin_stage(
                build_id,
                PipelineStage.PLANNING,
                worker_id=f"worker_{worker_id}"
            )
            
            with lock:
                if begin_result.result == StageBeginResult.PROCEED:
                    # Simulate planning work
                    time.sleep(0.05)
                    
                    # Mark stage as complete
                    coordinator.complete_stage(
                        build_id,
                        PipelineStage.PLANNING,
                        lock_token=begin_result.lock_token,
                    )
                    results["success"].append(build_id)
                elif begin_result.result == StageBeginResult.LOCKED:
                    results["locked"].append(build_id)
                else:
                    results["locked"].append(build_id)
        except Exception as e:
            with lock:
                results["errors"].append((build_id, str(e)))
    
    # Execute with 2 workers processing builds concurrently
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        for idx, build_id in enumerate(build_ids):
            worker_id = idx % 2  # Alternate between 2 workers
            future = executor.submit(worker_process_build, worker_id, build_id)
            futures.append(future)
        
        # Wait for all to complete
        for future in as_completed(futures):
            future.result()
    
    # Verify results
    assert len(results["errors"]) == 0, f"Errors occurred: {results['errors']}"
    assert len(results["success"]) == num_builds, f"Expected {num_builds} successes, got {len(results['success'])}"
    
    # Verify all stages are marked complete
    for build_id in results["success"]:
        assert verify_stage_done(db, build_id, PipelineStage.PLANNING)
    
    print(f"\n✓ Stress test passed:")
    print(f"  - Successful completions: {len(results['success'])}")
    print(f"  - Lock conflicts: {len(results['locked'])}")
    print(f"  - Errors: {len(results['errors'])}")


def test_planning_same_build_2worker_lock_contention(coordinator):
    """
    Stress test: 2 workers competing for the same build in PLANNING stage.
    Verifies that only one worker can proceed and other gets LOCKED.
    """
    db = coordinator.db
    build_id = f"contention_{uuid.uuid4().hex[:8]}"
    
    # Create single build
    db.create_build(
        build_id,
        {
            "code_type": "test",
            "description": "contention test",
            "request": {
                "tool_combinations": [{"name": "stack1"}],
                "factory_review": {"overall_score": 80},
            },
        },
    )
    db.save_checkpoint(build_id, {
        "tool_combinations": [{"name": "stack1"}],
        "factory_review": {"overall_score": 80},
    })
    
    results = {"proceeded": None, "locked": None, "errors": []}
    lock = threading.Lock()
    barrier = threading.Barrier(2)  # Synchronize both threads
    
    def worker_race(worker_id: int):
        """Worker thread that races to acquire lock."""
        try:
            barrier.wait()  # Ensure both workers start at same time
            
            begin_result = coordinator.begin_stage(
                build_id,
                PipelineStage.PLANNING,
                worker_id=f"worker_{worker_id}"
            )
            
            with lock:
                if begin_result.result == StageBeginResult.PROCEED:
                    results["proceeded"] = worker_id
                    # Hold lock briefly to ensure second worker sees LOCKED
                    time.sleep(0.1)
                    coordinator.complete_stage(
                        build_id,
                        PipelineStage.PLANNING,
                        lock_token=begin_result.lock_token,
                    )
                elif begin_result.result == StageBeginResult.LOCKED:
                    results["locked"] = worker_id
        except Exception as e:
            with lock:
                results["errors"].append((worker_id, str(e)))
    
    # Race 2 workers on same build
    with ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(worker_race, 1)
        f2 = executor.submit(worker_race, 2)
        f1.result()
        f2.result()
    
    # Verify exactly one proceeded and one was locked
    assert results["proceeded"] is not None, "No worker proceeded"
    assert results["locked"] is not None, "No worker was locked"
    assert results["proceeded"] != results["locked"], "Same worker can't both proceed and be locked"
    assert len(results["errors"]) == 0, f"Errors occurred: {results['errors']}"
    assert verify_stage_done(db, build_id, PipelineStage.PLANNING)
    
    print(f"\n✓ Lock contention test passed:")
    print(f"  - Worker {results['proceeded']} proceeded")
    print(f"  - Worker {results['locked']} was locked (as expected)")


def test_planning_stress_rapid_fire_attempts(coordinator):
    """
    Stress test: Rapid-fire sequential attempts to lock same build.
    Tests lock TTL and garbage collection.
    """
    db = coordinator.db
    build_id = f"rapid_{uuid.uuid4().hex[:8]}"
    
    db.create_build(
        build_id,
        {
            "code_type": "test",
            "description": "rapid fire test",
            "request": {
                "tool_combinations": [{"name": "stack1"}],
                "factory_review": {"overall_score": 80},
            },
        },
    )
    db.save_checkpoint(build_id, {
        "tool_combinations": [{"name": "stack1"}],
        "factory_review": {"overall_score": 80},
    })
    
    attempt_count = 20
    success_count = 0
    
    for attempt in range(attempt_count):
        begin_result = coordinator.begin_stage(
            build_id,
            PipelineStage.PLANNING,
            worker_id=f"rapid_worker_{attempt}"
        )
        
        if begin_result.result == StageBeginResult.PROCEED:
            success_count += 1
            # Complete stage
            coordinator.complete_stage(
                build_id,
                PipelineStage.PLANNING,
                lock_token=begin_result.lock_token,
            )
            # After first success, all subsequent should be ALREADY_DONE
            
    # Verify stage completed on first attempt
    assert success_count == 1, f"Expected exactly 1 successful attempt, got {success_count}"
    assert verify_stage_done(db, build_id, PipelineStage.PLANNING)
    
    print(f"\n✓ Rapid-fire test passed:")
    print(f"  - Total attempts: {attempt_count}")
    print(f"  - Successful acquisitions: {success_count}")
    print(f"  - Stage completed: ✓")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
