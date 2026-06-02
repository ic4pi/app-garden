#!/usr/bin/env python3
"""Fix builds stuck with Illegal transition errors by marking them interrupted for recovery.

Usage:
  python scripts/fix_illegal_transitions.py [--apply]

Without --apply the script only prints affected builds.
"""
import sqlite3
import argparse
from pathlib import Path

DB = Path(__file__).resolve().parents[1] / "data" / "gardener.db"

parser = argparse.ArgumentParser()
parser.add_argument("--apply", action="store_true", help="Apply fixes")
args = parser.parse_args()

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

q = "SELECT b.id, b.status, s.pipeline_status, s.message, s.resume_phase FROM builds b JOIN pipeline_state s ON b.id = s.build_id WHERE s.message LIKE 'Illegal transition %'"
rows = cur.execute(q).fetchall()
if not rows:
    print('No builds with Illegal transition found')
    exit(0)

print(f'Found {len(rows)} build(s) with Illegal transition:')
for r in rows:
    print(f" - {r['id']} status={r['status']} pipeline_status={r['pipeline_status']} resume_phase={r['resume_phase']}")

if not args.apply:
    print('\nRun with --apply to mark these builds interrupted and resume them')
    exit(0)

for r in rows:
    build_id = r['id']
    print('Fixing', build_id)
    # update builds table status to interrupted
    cur.execute("UPDATE builds SET status=? WHERE id=?", ('interrupted', build_id))
    # update pipeline_state: set pipeline_status to interrupted, resume_phase to the previous phase if available
    resume_phase = r['resume_phase'] if r['resume_phase'] and r['resume_phase'] != 'failed' else 'reviewing'
    cur.execute("UPDATE pipeline_state SET pipeline_status=?, message=?, percent=?, resume_phase=? WHERE build_id=?", ('interrupted', 'Recovered: marked interrupted for resume by fix_illegal_transitions', 0.0, resume_phase, build_id))
    # remove any running stage runs for this build so dispatcher can enqueue
    cur.execute("DELETE FROM pipeline_stage_runs WHERE build_id=? AND status='running'", (build_id,))

conn.commit()
print('Applied fixes; re-dispatch builds via /api or restart recovery task')
