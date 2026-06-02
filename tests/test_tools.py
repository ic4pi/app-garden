"""Tests for builder tooling layer (no full app import)."""

import asyncio
from pathlib import Path

from core.artifact_parser import parse_code_artifact
from core.config import Config
from core.tools.builder_toolkit import BuilderToolkit
from core.tools.registry import ToolRegistry
from core.tools.workspace import BuildWorkspace


def test_parse_and_write_tree(tmp_path):
    artifact = """```file: app.py
def main():
    return 42
```
```file: requirements.txt
fastapi>=0.1.0
```"""
    files = parse_code_artifact(artifact)
    assert "app.py" in files
    ws = BuildWorkspace(tmp_path / "b1" / "a1")
    written = ws.write_files(files)
    assert len(written) == 2
    assert (ws.src / "app.py").exists()
    rebuilt = ws.to_artifact()
    assert "app.py" in rebuilt


def test_filesystem_tool_list():
    async def _run():
        ws = BuildWorkspace(Path(Config.WORKSPACES_DIR) / "_test_tools" / "t1")
        ws.write_files({"hello.py": "x = 1\n"})
        toolkit = BuilderToolkit(ws, build_id="_test_tools", attempt_id="t1")
        result = await toolkit.registry.run(
            toolkit._ctx, "filesystem", action="list", path=""
        )
        assert result.ok
        assert "hello.py" in result.data.get("files", [])

    asyncio.run(_run())


def test_shell_blocks_unsafe():
    async def _run():
        ws = BuildWorkspace(Path(Config.WORKSPACES_DIR) / "_test_tools" / "t2")
        toolkit = BuilderToolkit(ws, build_id="_test_tools", attempt_id="t2")
        result = await toolkit.registry.run(
            toolkit._ctx, "shell", command="rm -rf /"
        )
        assert not result.ok

    asyncio.run(_run())


def test_materialize_from_artifact(tmp_path):
    async def _run():
        artifact = """```file: main.py
print("ok")
```"""
        ws = BuildWorkspace(tmp_path / "build_x" / "attempt_y")
        toolkit = BuilderToolkit(ws, build_id="build_x", attempt_id="attempt_y")
        result = await toolkit.materialize_from_artifact(artifact)
        assert result.ok
        assert result.files_written == 1
        assert (ws.src / "main.py").read_text().strip() == 'print("ok")'

    asyncio.run(_run())
