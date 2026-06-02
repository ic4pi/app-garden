"""Parse multi-file code artifacts from builder LLM output."""

from __future__ import annotations

import re
from typing import Dict


def parse_code_artifact(code_artifact: str) -> Dict[str, str]:
    """Extract filename → content map from artifact text."""
    files: Dict[str, str] = {}
    pattern = r"```(?:file:\s*)?([^\n]+)\n(.*?)```"
    matches = re.findall(pattern, code_artifact, re.DOTALL)
    if matches:
        for filename, content in matches:
            filename = filename.strip()
            if filename and content.strip():
                files[filename] = content.strip()
        return files

    pattern2 = r"(?:^|\n)//?\s*FILE:\s*([^\n]+)\n(.*?)(?=\n//?\s*FILE:|$)"
    matches2 = re.findall(pattern2, code_artifact, re.DOTALL | re.IGNORECASE)
    if matches2:
        for filename, content in matches2:
            filename = filename.strip()
            if filename and content.strip():
                files[filename] = content.strip()
        return files

    if code_artifact.strip():
        files["main.py"] = code_artifact.strip()
    return files


def serialize_code_artifact(files: Dict[str, str]) -> str:
    """Rebuild artifact text from a file map."""
    parts = []
    for name, content in sorted(files.items()):
        parts.append(f"```file: {name}\n{content.rstrip()}\n```")
    return "\n\n".join(parts)
