#!/usr/bin/env python3
"""
THE GARDENER - Multi-Agent Code Generation & Curation Platform
A meta-application that creates specialized AI agents for code generation,
review, and ranking using a hierarchical pipeline architecture.

Architecture:
  1. META-BUILDER ("The Gardener") - Orchestrator that "plants" tools
  2. BUILDER BOT (5 Attempts) - Generates functional code
  3. LLM REVIEWER - Detailed critique & analysis
  4. RANKER - 0-100 scoring with weighted criteria
  5. NOVELTY SITE BUILDER (3 Attempts) - Final deliverables
  6. LEADERBOARD SYSTEM - SQLite persistence, filterable tables
  7. DOWNLOAD MANAGER - ZIP packaging with README + requirements

API Integration:
  - NVIDIA API: Primary builder & reviewer (Nemotron 3 Super)
  - MiniMax M2.5 (OpenRouter): Secondary builder for variations
  - OpenRouter Free Tier: Ranker (meta-llama/llama-4-maverick:free)

Tech Stack: Python 3.11+, FastAPI, SQLite, AsyncIO, Pydantic, Jinja2
"""

import os
import sys
import json
import time
import asyncio
import sqlite3
import zipfile
import shutil
import uuid
import re
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Any
from enum import Enum

from fastapi import FastAPI, Request, Form, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from pydantic import BaseModel, Field
import httpx

from fastapi.middleware.cors import CORSMiddleware
from core import api_keys as api_keys
from core.database import get_database

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

class Config:
    NVIDIA_API_KEY = os.getenv("NVIDIA_API_KEY", "")
    OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
    NVIDIA_BUILDER_MODEL = os.getenv("NVIDIA_BUILDER_MODEL", "nvidia/nemotron-3-super")
    NVIDIA_REVIEWER_MODEL = os.getenv("NVIDIA_REVIEWER_MODEL", "nvidia/nemotron-3-super")
    MINIMAX_MODEL = os.getenv("MINIMAX_MODEL", "minimax/minimax-m2.5")
    RANKER_MODEL = os.getenv("RANKER_MODEL", "meta-llama/llama-4-maverick:free")
    FALLBACK_RANKER = os.getenv("FALLBACK_RANKER", "deepseek/deepseek-r1:free")
    OPENROUTER_RETRY_COUNT = int(os.getenv("OPENROUTER_RETRY_COUNT", "5"))
    OPENROUTER_RETRY_BACKOFF = float(os.getenv("OPENROUTER_RETRY_BACKOFF", "2.0"))
    NVIDIA_API_URL = "https://api.nvidia.com/v1/chat/completions"
    OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
    REQUEST_TIMEOUT = 120
    DB_PATH = os.getenv("DB_PATH", "gardener.db")
    OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./outputs"))
    STATIC_DIR = Path(os.getenv("STATIC_DIR", "./static"))
    OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    OLLAMA_CLOUD_HOST = os.getenv("OLLAMA_CLOUD_HOST", "")

    @classmethod
    def update_from_settings(cls, data: dict[str, Any]) -> None:
        for key, value in data.items():
            if not isinstance(key, str):
                continue
            attr = key.upper()
            if not hasattr(cls, attr):
                continue
            current_value = getattr(cls, attr)
            if isinstance(current_value, Path) and isinstance(value, str):
                setattr(cls, attr, Path(value))
            else:
                setattr(cls, attr, value)

    @classmethod
    def to_public_dict(cls) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for name in dir(cls):
            if not name.isupper() or name.startswith("_"):
                continue
            value = getattr(cls, name)
            if isinstance(value, Path):
                value = str(value)
            if isinstance(value, (list, dict, tuple)):
                result[name.lower()] = value
            elif isinstance(value, (bool, int, float)):
                result[name.lower()] = value
            else:
                result[name.lower()] = cls._mask_secret_value(name, str(value))
        return result

    @classmethod
    def _mask_secret_value(cls, name: str, value: str) -> str:
        if not value:
            return ""
        secret_keys = ("API_KEY", "KEY", "SECRET")
        if any(token in name for token in secret_keys):
            return value[:6] + "..." + value[-4:] if len(value) > 12 else "*****"
        return value

Config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
Config.STATIC_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════════════════════
#  DATA MODELS
# ═══════════════════════════════════════════════════════════════════════════════

class CodeType(str, Enum):
    WEBSITE = "website"
    WEB_APP = "web_app"
    API_BACKEND = "api_backend"
    CLI_TOOL = "cli_tool"
    DATA_PIPELINE = "data_pipeline"
    GAME = "game"
    MOBILE_APP = "mobile_app"
    CHATBOT = "chatbot"
    DASHBOARD = "dashboard"
    E_COMMERCE = "e_commerce"
    PORTFOLIO = "portfolio"
    BLOG = "blog"
    CUSTOM = "custom"

class ToolStack(BaseModel):
    name: str
    frontend: List[str] = Field(default_factory=list)
    backend: List[str] = Field(default_factory=list)
    database: List[str] = Field(default_factory=list)
    styling: List[str] = Field(default_factory=list)
    utilities: List[str] = Field(default_factory=list)
    deployment: List[str] = Field(default_factory=list)
    justification: str = ""
    novelty_score: int = Field(0, ge=0, le=100)

class BuildAttempt(BaseModel):
    attempt_id: str
    attempt_number: int
    tool_stack: ToolStack
    model_used: str
    code_artifact: str = ""
    build_log: str = ""
    tool_usage_report: str = ""
    build_time_seconds: float = 0.0
    success: bool = False
    error_message: str = ""
    timestamp: str = ""

class ReviewDimension(BaseModel):
    dimension: str
    score: int = Field(0, ge=0, le=100)
    analysis: str = ""
    suggestions: List[str] = Field(default_factory=list)

class ReviewReport(BaseModel):
    attempt_id: str
    overall_score: int = Field(0, ge=0, le=100)
    dimensions: List[ReviewDimension] = Field(default_factory=list)
    comparative_notes: str = ""
    what_works_better: str = ""
    improvement_suggestions: List[str] = Field(default_factory=list)
    potential_failure_points: List[str] = Field(default_factory=list)
    timestamp: str = ""

class RankedBuild(BaseModel):
    attempt_id: str
    attempt_number: int
    tool_stack_name: str
    functionality_score: int = Field(0, ge=0, le=100)
    code_quality_score: int = Field(0, ge=0, le=100)
    tool_optimization_score: int = Field(0, ge=0, le=100)
    novelty_score: int = Field(0, ge=0, le=100)
    documentation_score: int = Field(0, ge=0, le=100)
    total_score: float = 0.0
    justification: str = ""
    rank: int = 0

class NoveltyAttempt(BaseModel):
    attempt_id: str
    iteration: int
    winning_config: ToolStack
    code_artifact: str = ""
    build_log: str = ""
    creativity_notes: str = ""
    build_time_seconds: float = 0.0
    success: bool = False
    timestamp: str = ""

class LeaderboardEntry(BaseModel):
    entry_id: str
    project_name: str
    code_type: str
    score: float
    novelty_rating: int
    tool_stack: str
    build_time_seconds: float
    user_rating: Optional[int] = None
    created_at: str
    download_path: Optional[str] = None
    model_used: str = ""

class BuildRequest(BaseModel):
    code_type: CodeType
    description: str = Field(..., min_length=10, max_length=2000)
    specific_requirements: str = ""
    preferred_frameworks: List[str] = Field(default_factory=list)
    target_audience: str = ""
    complexity_level: str = "medium"


# ═══════════════════════════════════════════════════════════════════════════════
#  TOOL INVENTORY
# ═══════════════════════════════════════════════════════════════════════════════

class ToolInventory:
    FRONTEND_TOOLS = {
        "react": {"best_for": ["web_app", "dashboard", "e_commerce"], "synergy": ["nextjs", "tailwind", "typescript"]},
        "vue": {"best_for": ["web_app", "dashboard", "portfolio"], "synergy": ["nuxt", "vuetify", "pinia"]},
        "svelte": {"best_for": ["website", "portfolio", "blog"], "synergy": ["sveltekit", "tailwind", "typescript"]},
        "angular": {"best_for": ["e_commerce", "enterprise", "dashboard"], "synergy": ["rxjs", "material", "typescript"]},
        "nextjs": {"best_for": ["web_app", "e_commerce", "blog"], "synergy": ["react", "tailwind", "vercel"]},
        "nuxt": {"best_for": ["web_app", "portfolio", "blog"], "synergy": ["vue", "tailwind", "netlify"]},
        "astro": {"best_for": ["website", "blog", "portfolio"], "synergy": ["react", "vue", "tailwind"]},
        "htmx": {"best_for": ["website", "dashboard", "blog"], "synergy": ["django", "flask", "alpine"]},
        "alpinejs": {"best_for": ["website", "portfolio", "blog"], "synergy": ["tailwind", "htmx", "django"]},
        "threejs": {"best_for": ["game", "portfolio", "dashboard"], "synergy": ["react", "webgl", "gsap"]},
        "d3": {"best_for": ["dashboard", "data_pipeline", "website"], "synergy": ["react", "svelte", "typescript"]},
        "flutter_web": {"best_for": ["mobile_app", "web_app"], "synergy": ["dart", "firebase"]},
    }

    BACKEND_TOOLS = {
        "fastapi": {"best_for": ["api_backend", "web_app", "dashboard"], "synergy": ["sqlalchemy", "pydantic", "uvicorn"]},
        "django": {"best_for": ["e_commerce", "web_app", "blog"], "synergy": ["htmx", "tailwind", "postgres"]},
        "flask": {"best_for": ["api_backend", "web_app", "cli_tool"], "synergy": ["sqlalchemy", "jinja2", "gunicorn"]},
        "express": {"best_for": ["api_backend", "web_app", "e_commerce"], "synergy": ["mongodb", "typescript", "socket.io"]},
        "spring_boot": {"best_for": ["api_backend", "e_commerce", "enterprise"], "synergy": ["postgres", "redis", "docker"]},
        "go_gin": {"best_for": ["api_backend", "cli_tool", "high_performance"], "synergy": ["postgres", "redis", "docker"]},
        "rust_axum": {"best_for": ["api_backend", "high_performance", "web_app"], "synergy": ["sqlx", "tokio", "docker"]},
        "graphql_apollo": {"best_for": ["api_backend", "web_app", "dashboard"], "synergy": ["react", "prisma", "postgres"]},
        "websocket": {"best_for": ["chatbot", "game", "dashboard"], "synergy": ["socket.io", "redis", "fastapi"]},
        "serverless": {"best_for": ["api_backend", "web_app", "cli_tool"], "synergy": ["aws_lambda", "vercel", "dynamodb"]},
    }

    DATABASE_TOOLS = {
        "postgres": {"best_for": ["e_commerce", "web_app", "blog"], "synergy": ["sqlalchemy", "prisma", "redis"]},
        "mongodb": {"best_for": ["web_app", "chatbot", "dashboard"], "synergy": ["mongoose", "express", "redis"]},
        "sqlite": {"best_for": ["cli_tool", "prototype", "small_app"], "synergy": ["sqlalchemy", "flask", "fastapi"]},
        "redis": {"best_for": ["api_backend", "chatbot", "game"], "synergy": ["postgres", "fastapi", "docker"]},
        "firebase": {"best_for": ["mobile_app", "web_app", "chatbot"], "synergy": ["flutter", "react", "google_cloud"]},
        "supabase": {"best_for": ["web_app", "e_commerce", "blog"], "synergy": ["postgres", "nextjs", "tailwind"]},
        "prisma": {"best_for": ["web_app", "api_backend", "dashboard"], "synergy": ["nextjs", "postgres", "typescript"]},
        "sqlalchemy": {"best_for": ["api_backend", "web_app", "data_pipeline"], "synergy": ["fastapi", "postgres", "flask"]},
        "dynamodb": {"best_for": ["serverless", "web_app", "game"], "synergy": ["aws_lambda", "express", "serverless"]},
    }

    STYLING_TOOLS = {
        "tailwind": {"best_for": ["website", "web_app", "dashboard"], "synergy": ["react", "vue", "nextjs"]},
        "bootstrap": {"best_for": ["website", "dashboard", "e_commerce"], "synergy": ["react", "django", "flask"]},
        "sass": {"best_for": ["website", "portfolio", "blog"], "synergy": ["react", "vue", "angular"]},
        "styled_components": {"best_for": ["web_app", "dashboard", "e_commerce"], "synergy": ["react", "nextjs", "typescript"]},
        "framer_motion": {"best_for": ["portfolio", "web_app", "website"], "synergy": ["react", "tailwind", "nextjs"]},
        "gsap": {"best_for": ["portfolio", "game", "website"], "synergy": ["threejs", "react", "svelte"]},
        "shadcn": {"best_for": ["dashboard", "web_app", "e_commerce"], "synergy": ["react", "tailwind", "nextjs"]},
        "material_ui": {"best_for": ["dashboard", "web_app", "e_commerce"], "synergy": ["react", "nextjs", "typescript"]},
    }

    UTILITY_TOOLS = {
        "typescript": {"best_for": ["web_app", "api_backend", "dashboard"], "synergy": ["react", "nextjs", "express"]},
        "zod": {"best_for": ["api_backend", "web_app", "cli_tool"], "synergy": ["typescript", "nextjs", "fastapi"]},
        "pytest": {"best_for": ["api_backend", "cli_tool", "data_pipeline"], "synergy": ["fastapi", "django", "flask"]},
        "jest": {"best_for": ["web_app", "api_backend", "dashboard"], "synergy": ["react", "nextjs", "express"]},
        "docker": {"best_for": ["api_backend", "web_app", "e_commerce"], "synergy": ["postgres", "redis", "nginx"]},
        "nginx": {"best_for": ["web_app", "api_backend", "e_commerce"], "synergy": ["docker", "ssl", "load_balancer"]},
        "auth_jwt": {"best_for": ["api_backend", "web_app", "e_commerce"], "synergy": ["fastapi", "express", "nextjs"]},
        "stripe": {"best_for": ["e_commerce", "web_app", "saas"], "synergy": ["nextjs", "express", "postgres"]},
        "openai_api": {"best_for": ["chatbot", "web_app", "dashboard"], "synergy": ["fastapi", "react", "nextjs"]},
        "langchain": {"best_for": ["chatbot", "data_pipeline", "web_app"], "synergy": ["fastapi", "openai_api", "postgres"]},
        "pandas": {"best_for": ["data_pipeline", "dashboard", "cli_tool"], "synergy": ["fastapi", "postgres", "streamlit"]},
        "streamlit": {"best_for": ["dashboard", "data_pipeline", "prototype"], "synergy": ["pandas", "plotly", "fastapi"]},
    }

    @classmethod
    def get_tools_for_type(cls, code_type: str) -> Dict[str, List[str]]:
        result = {"frontend": [], "backend": [], "database": [], "styling": [], "utility": []}
        for name, meta in cls.FRONTEND_TOOLS.items():
            if code_type in meta["best_for"]: result["frontend"].append(name)
        for name, meta in cls.BACKEND_TOOLS.items():
            if code_type in meta["best_for"]: result["backend"].append(name)
        for name, meta in cls.DATABASE_TOOLS.items():
            if code_type in meta["best_for"]: result["database"].append(name)
        for name, meta in cls.STYLING_TOOLS.items():
            if code_type in meta["best_for"]: result["styling"].append(name)
        for name, meta in cls.UTILITY_TOOLS.items():
            if code_type in meta["best_for"]: result["utility"].append(name)
        return result

    @classmethod
    def generate_justification(cls, stack: ToolStack, code_type: str) -> str:
        all_tools = {**cls.FRONTEND_TOOLS, **cls.BACKEND_TOOLS, **cls.DATABASE_TOOLS,
                     **cls.STYLING_TOOLS, **cls.UTILITY_TOOLS}
        parts = []
        all_selected = stack.frontend + stack.backend + stack.database + stack.styling + stack.utilities

        parts.append(f"Planting Philosophy: Stack cultivated for '{code_type}' project.")
        parts.append(f"Selection prioritizes {'performance' if 'rust' in str(all_selected).lower() else 'developer experience' if 'react' in str(all_selected).lower() else 'simplicity and speed'}.")

        if stack.frontend:
            frontend = stack.frontend[0]
            meta = all_tools.get(frontend, {})
            parts.append(f"Frontend: {frontend} - excels at {', '.join(meta.get('best_for', ['general use'])[:2])}.")
            synergies = [s for s in meta.get('synergy', []) if s in all_selected]
            if synergies: parts.append(f"Natural synergy with: {', '.join(synergies)}")

        if stack.backend:
            backend = stack.backend[0]
            meta = all_tools.get(backend, {})
            parts.append(f"Backend: {backend} - strength in {', '.join(meta.get('best_for', ['general use'])[:2])}.")
            synergies = [s for s in meta.get('synergy', []) if s in all_selected]
            if synergies: parts.append(f"Complements stack via: {', '.join(synergies)}")

        if stack.database:
            parts.append(f"Database: {stack.database[0]} - matched to {code_type} data patterns.")
        if stack.styling:
            parts.append(f"Styling: {stack.styling[0]} - appropriate visual language.")

        novelty_indicators = []
        if "rust" in str(all_selected).lower(): novelty_indicators.append("Rust ecosystem")
        if "htmx" in str(all_selected).lower(): novelty_indicators.append("Hypermedia architecture")
        if "threejs" in str(all_selected).lower(): novelty_indicators.append("3D/webGL integration")
        if "langchain" in str(all_selected).lower(): novelty_indicators.append("AI-native architecture")
        if novelty_indicators: parts.append(f"Novelty: {', '.join(novelty_indicators)}")

        return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════════════════════
#  LLM CLIENT
# ═══════════════════════════════════════════════════════════════════════════════

from core.pipeline_domain import LLMClient

# ═══════════════════════════════════════════════════════════════════════════════
#  META-BUILDER ("The Gardener")
# ═══════════════════════════════════════════════════════════════════════════════

class MetaBuilder:
    def __init__(self, llm_client):
        self.llm = llm_client
        self.inventory = ToolInventory()

    def _generate_variation_1_classic_fullstack(self, code_type, tools):
        stack = ToolStack(
            name="Classic Fullstack",
            frontend=["react", "nextjs"] if "nextjs" in tools["frontend"] else ["react"],
            backend=["fastapi"] if "fastapi" in tools["backend"] else [tools["backend"][0] if tools["backend"] else "flask"],
            database=["postgres", "sqlalchemy"] if "postgres" in tools["database"] else [tools["database"][0] if tools["database"] else "sqlite"],
            styling=["tailwind", "shadcn"] if "tailwind" in tools["styling"] else [tools["styling"][0] if tools["styling"] else "bootstrap"],
            utilities=["typescript", "zod", "docker"] if "typescript" in tools["utility"] else ["docker"],
            deployment=["docker", "nginx"], novelty_score=45
        )
        stack.justification = self.inventory.generate_justification(stack, code_type)
        return stack

    def _generate_variation_2_minimalist_hypermedia(self, code_type, tools):
        stack = ToolStack(
            name="Minimalist Hypermedia",
            frontend=["htmx", "alpinejs"] if "htmx" in tools["frontend"] else ["svelte"],
            backend=["django"] if "django" in tools["backend"] else ["flask"],
            database=["sqlite"] if "sqlite" in tools["database"] else ["postgres"],
            styling=["tailwind"] if "tailwind" in tools["styling"] else ["bootstrap"],
            utilities=["auth_jwt", "pytest"], deployment=["docker"], novelty_score=65
        )
        stack.justification = self.inventory.generate_justification(stack, code_type)
        return stack

    def _generate_variation_3_ai_native(self, code_type, tools):
        stack = ToolStack(
            name="AI-Native Architecture",
            frontend=["react", "nextjs"] if "react" in tools["frontend"] else ["vue"],
            backend=["fastapi", "graphql_apollo"] if "fastapi" in tools["backend"] else ["express"],
            database=["supabase", "prisma"] if "supabase" in tools["database"] else ["postgres", "prisma"],
            styling=["tailwind", "framer_motion"] if "framer_motion" in tools["styling"] else ["tailwind"],
            utilities=["langchain", "openai_api", "typescript", "zod"],
            deployment=["docker", "serverless"], novelty_score=85
        )
        stack.justification = self.inventory.generate_justification(stack, code_type)
        return stack

    def _generate_variation_4_performance_rust(self, code_type, tools):
        stack = ToolStack(
            name="High-Performance Rust",
            frontend=["svelte", "htmx"] if "svelte" in tools["frontend"] else ["react"],
            backend=["rust_axum"],
            database=["postgres", "redis"] if "redis" in tools["database"] else ["postgres"],
            styling=["tailwind", "gsap"] if "gsap" in tools["styling"] else ["tailwind"],
            utilities=["docker", "nginx", "auth_jwt"],
            deployment=["docker", "nginx"], novelty_score=90
        )
        stack.justification = self.inventory.generate_justification(stack, code_type)
        return stack

    def _generate_variation_5_realtime_collaborative(self, code_type, tools):
        stack = ToolStack(
            name="Real-Time Collaborative",
            frontend=["vue", "nuxt"] if "vue" in tools["frontend"] else ["react"],
            backend=["express", "websocket"] if "express" in tools["backend"] else ["fastapi", "websocket"],
            database=["mongodb", "redis"] if "mongodb" in tools["database"] else ["postgres", "redis"],
            styling=["tailwind", "material_ui"] if "material_ui" in tools["styling"] else ["tailwind"],
            utilities=["typescript", "docker", "auth_jwt"],
            deployment=["docker", "serverless"], novelty_score=75
        )
        stack.justification = self.inventory.generate_justification(stack, code_type)
        return stack

    def generate_tool_combinations(self, code_type, preferred=[]):
        tools = self.inventory.get_tools_for_type(code_type)
        if preferred:
            for category in tools:
                preferred_in = [p for p in preferred if p in tools[category]]
                if preferred_in:
                    tools[category] = preferred_in + [t for t in tools[category] if t not in preferred_in]
        return [
            self._generate_variation_1_classic_fullstack(code_type, tools),
            self._generate_variation_2_minimalist_hypermedia(code_type, tools),
            self._generate_variation_3_ai_native(code_type, tools),
            self._generate_variation_4_performance_rust(code_type, tools),
            self._generate_variation_5_realtime_collaborative(code_type, tools),
        ]

# ═══════════════════════════════════════════════════════════════════════════════
#  BUILDER BOT
# ═══════════════════════════════════════════════════════════════════════════════

class BuilderBot:
    SYSTEM_PROMPT = """You are an expert software architect. Generate COMPLETE, production-ready code.
RULES:
1. Generate COMPLETE, runnable code
2. Include all necessary files
3. Use ONLY assigned tools/frameworks
4. Provide file structure comments
5. Include error handling and validation
6. Write clean, well-commented code
7. Include README as comments
8. Make code NOVEL and CREATIVE
OUTPUT FORMAT:
```file: filename.ext
// code content
```"""

    def __init__(self, llm_client):
        self.llm = llm_client

    async def build(self, request, stack, attempt_number):
        attempt_id = f"build_{uuid.uuid4().hex[:8]}"
        start_time = time.time()
        model = Config.NVIDIA_BUILDER_MODEL if attempt_number <= 2 else Config.MINIMAX_MODEL
        model_name = "NVIDIA Nemotron 3 Super" if attempt_number <= 2 else "MiniMax M2.5"
        prompt = self._construct_build_prompt(request, stack, attempt_number)

        build_log = f"[{datetime.now().isoformat()}] Starting build attempt {attempt_number}\n"
        build_log += f"[{datetime.now().isoformat()}] Stack: {stack.name} | Model: {model_name}\n"

        try:
            code_artifact = await self.llm.generate_code(prompt, model, self.SYSTEM_PROMPT)
            if code_artifact.startswith("ERROR:"):
                success, error = False, code_artifact
                build_log += f"[{datetime.now().isoformat()}] FAILED: {error}\n"
            else:
                success, error = True, ""
                build_log += f"[{datetime.now().isoformat()}] SUCCESS: {len(code_artifact)} chars\n"
        except Exception as e:
            code_artifact, success, error = "", False, str(e)
            build_log += f"[{datetime.now().isoformat()}] EXCEPTION: {error}\n"

        build_time = time.time() - start_time
        build_log += f"[{datetime.now().isoformat()}] Completed in {build_time:.2f}s\n"

        return BuildAttempt(
            attempt_id=attempt_id, attempt_number=attempt_number, tool_stack=stack,
            model_used=model_name, code_artifact=code_artifact, build_log=build_log,
            tool_usage_report=self._generate_tool_report(stack, code_artifact, success),
            build_time_seconds=build_time, success=success, error_message=error,
            timestamp=datetime.now().isoformat()
        )

    def _construct_build_prompt(self, request, stack, attempt_number):
        return f"""# BUILD REQUEST
## Project Type: {request.code_type.value}
## Description: {request.description}
## Requirements: {request.specific_requirements or "None"}
## Audience: {request.target_audience or "General"}
## Complexity: {request.complexity_level}

## TOOL STACK (Attempt #{attempt_number}): {stack.name}
### Frontend:
""" + "\n".join(f"- {t}" for t in stack.frontend) + """
### Backend:
""" + "\n".join(f"- {t}" for t in stack.backend) + """
### Database:
""" + "\n".join(f"- {t}" for t in stack.database) + """
### Styling:
""" + "\n".join(f"- {t}" for t in stack.styling) + """
### Utilities:
""" + "\n".join(f"- {t}" for t in stack.utilities) + """
### Deployment:
""" + "\n".join(f"- {t}" for t in stack.deployment) + f"""

## JUSTIFICATION:
{stack.justification}

Generate COMPLETE, production-ready code using ONLY these tools."""

    def _generate_tool_report(self, stack, code, success):
        report = f"Tool Usage Report for {stack.name}\n"
        for tool in stack.frontend + stack.backend + stack.database + stack.styling + stack.utilities:
            mentions = code.lower().count(tool.lower().replace("_", " "))
            report += f"{tool}: {mentions} mentions - {'Used' if mentions > 0 else 'Not detected'}\n"
        report += f"Status: {'SUCCESS' if success else 'FAILED'}"
        return report


# ═══════════════════════════════════════════════════════════════════════════════
#  LLM REVIEWER
# ═══════════════════════════════════════════════════════════════════════════════

class LLMReviewer:
    SYSTEM_PROMPT = """You are a senior code reviewer with 20+ years of experience.
Analyze code across: Code Correctness, Tool Efficiency, Architecture, Novelty, Failure Points.
Be thorough, specific, and constructive. Compare attempts. Output structured analysis."""

    def __init__(self, llm_client):
        self.llm = llm_client

    async def review_all(self, request, attempts):
        return [await self._review_single(request, a, attempts) for a in attempts]

    async def _review_single(self, request, attempt, all_attempts):
        prompt = self._construct_review_prompt(request, attempt, all_attempts)
        try:
            review_text = await self.llm.generate_code(prompt, Config.NVIDIA_REVIEWER_MODEL, self.SYSTEM_PROMPT)
            return self._parse_review(review_text, attempt.attempt_id)
        except Exception as e:
            return self._fallback_review(attempt.attempt_id, str(e))

    def _construct_review_prompt(self, request, attempt, all_attempts):
        others = [a for a in all_attempts if a.attempt_id != attempt.attempt_id]
        return f"""# CODE REVIEW
## Request: {request.code_type.value} - {request.description}
## Attempt #{attempt.attempt_number} | Stack: {attempt.tool_stack.name} | Success: {attempt.success}
## Code (first 3000 chars):
```
{attempt.code_artifact[:3000]}
```
## Other Attempts:
""" + "\n".join(f"- #{a.attempt_number}: {a.tool_stack.name}" for a in others) + f"""

Provide scores 0-100 for: Code Correctness, Tool Efficiency, Architecture, Novelty, Documentation.
Include comparative analysis and improvement suggestions."""

    def _parse_review(self, text, attempt_id):
        dim_names = [
            ("Code Correctness", ["correctness", "syntax", "valid", "compile"]),
            ("Tool Efficiency", ["efficiency", "combination", "synergy", "tools"]),
            ("Architecture", ["architecture", "appropriate", "structure", "design"]),
            ("Novelty", ["novelty", "innovation", "creative", "unique"]),
            ("Documentation", ["documentation", "comments", "readme", "explain"])
        ]
        dimensions = []
        for dim_name, keywords in dim_names:
            score = 70
            for keyword in keywords:
                patterns = [rf'{keyword}.*?[:\-]?\s*(\d{{1,3}})', rf'(\d{{1,3}})\s*[:\-/]?\s*100.*?{keyword}']
                for pattern in patterns:
                    match = re.search(pattern, text.lower())
                    if match:
                        try: score = max(0, min(100, int(match.group(1)))); break
                        except: pass
            dimensions.append(ReviewDimension(dimension=dim_name, score=score, analysis=f"Review for {dim_name}", suggestions=[]))
        overall = sum(d.score for d in dimensions) // len(dimensions) if dimensions else 50
        return ReviewReport(attempt_id=attempt_id, overall_score=overall, dimensions=dimensions,
                           comparative_notes="See review text", what_works_better="See review text",
                           improvement_suggestions=["See review text"], potential_failure_points=["See review text"],
                           timestamp=datetime.now().isoformat())

    def _fallback_review(self, attempt_id, error):
        return ReviewReport(attempt_id=attempt_id, overall_score=50,
                           dimensions=[ReviewDimension(dimension=d, score=50, analysis=f"Review failed: {error}", suggestions=[]) 
                                      for d in ["Code Correctness", "Tool Efficiency", "Architecture", "Novelty", "Documentation"]],
                           comparative_notes="Review failed", what_works_better="Unknown",
                           improvement_suggestions=["Retry review"], potential_failure_points=["Review system failure"],
                           timestamp=datetime.now().isoformat())

# ═══════════════════════════════════════════════════════════════════════════════
#  RANKER
# ═══════════════════════════════════════════════════════════════════════════════

class Ranker:
    WEIGHTS = {"functionality": 0.30, "code_quality": 0.25, "tool_optimization": 0.20, "novelty": 0.15, "documentation": 0.10}
    SYSTEM_PROMPT = """You are an expert code evaluator. Score builds 0-100 across:
Functionality(30%), Code Quality(25%), Tool Optimization(20%), Novelty(15%), Documentation(10%).
100 is theoretically perfect. Be critical.
Output: FUNCTIONALITY: [score] - [justification] etc. TOTAL: [weighted]. JUSTIFICATION: [reasoning]"""

    def __init__(self, llm_client):
        self.llm = llm_client

    async def rank_all(self, attempts, reviews):
        ranked = [await self._rank_single(a, r, attempts) for a, r in zip(attempts, reviews)]
        ranked.sort(key=lambda x: x.total_score, reverse=True)
        for i, r in enumerate(ranked, 1): r.rank = i
        return ranked

    async def _rank_single(self, attempt, review, all_attempts):
        prompt = self._construct_ranking_prompt(attempt, review, all_attempts)
        try:
            ranking_text = await self.llm.generate_code(prompt, Config.RANKER_MODEL, self.SYSTEM_PROMPT)
            return self._parse_ranking(ranking_text, attempt)
        except Exception as e:
            return self._fallback_ranking(attempt, review, str(e))

    def _construct_ranking_prompt(self, attempt, review, all_attempts):
        return f"""# RANKING
## Attempt #{attempt.attempt_number} | Stack: {attempt.tool_stack.name} | Success: {attempt.success}
## Review Scores:
""" + "\n".join(f"- {d.dimension}: {d.score}" for d in review.dimensions) + f"""
## Code Preview (first 2000 chars):
```
{attempt.code_artifact[:2000]}
```
Score and justify."""

    def _parse_ranking(self, text, attempt):
        def extract_score(label):
            for pattern in [rf'{label}[:\s]+(\d+)', rf'{label.replace("_", " ")}[:\s]+(\d+)']:
                match = re.search(pattern, text.lower())
                if match:
                    try: return max(0, min(100, int(match.group(1))))
                    except: pass
            return 70

        func = extract_score("functionality")
        quality = extract_score("code_quality")
        tool = extract_score("tool_optimization")
        novelty = extract_score("novelty")
        doc = extract_score("documentation")

        total_match = re.search(r'total[:\s]+([\d.]+)', text.lower())
        total = float(total_match.group(1)) if total_match else (
            func * 0.30 + quality * 0.25 + tool * 0.20 + novelty * 0.15 + doc * 0.10)

        return RankedBuild(attempt_id=attempt.attempt_id, attempt_number=attempt.attempt_number,
                          tool_stack_name=attempt.tool_stack.name, functionality_score=func,
                          code_quality_score=quality, tool_optimization_score=tool,
                          novelty_score=novelty, documentation_score=doc,
                          total_score=round(total, 2), justification=text[:500], rank=0)

    def _fallback_ranking(self, attempt, review, error):
        dim_map = {d.dimension: d.score for d in review.dimensions}
        func = dim_map.get("Code Correctness", 70)
        quality = dim_map.get("Tool Efficiency", 70)
        tool = dim_map.get("Architecture", 70)
        novelty = dim_map.get("Novelty", 70)
        doc = dim_map.get("Documentation", 70)
        total = func * 0.30 + quality * 0.25 + tool * 0.20 + novelty * 0.15 + doc * 0.10
        return RankedBuild(attempt_id=attempt.attempt_id, attempt_number=attempt.attempt_number,
                          tool_stack_name=attempt.tool_stack.name, functionality_score=func,
                          code_quality_score=quality, tool_optimization_score=tool,
                          novelty_score=novelty, documentation_score=doc,
                          total_score=round(total, 2), justification=f"Fallback: {error}", rank=0)

# ═══════════════════════════════════════════════════════════════════════════════
#  NOVELTY SITE BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

class NoveltySiteBuilder:
    SYSTEM_PROMPT = """You are a creative coding virtuoso. Build the most NOVEL, CREATIVE, 
INNOVATIVE version possible. Push boundaries. Use unexpected patterns. Include delightful 
micro-interactions. Make code a work of art. Each iteration MORE creative than last.
Generate COMPLETE, runnable code."""

    def __init__(self, llm_client):
        self.llm = llm_client

    async def build_novelty_versions(self, request, winning_stack, winning_attempt):
        attempts = []
        prev_code = winning_attempt.code_artifact
        for iteration in range(1, 4):
            attempt = await self._build_iteration(request, winning_stack, iteration, prev_code, attempts)
            attempts.append(attempt)
            if attempt.success: prev_code = attempt.code_artifact
        return attempts

    async def _build_iteration(self, request, stack, iteration, prev_code, prev_attempts):
        attempt_id = f"novelty_{uuid.uuid4().hex[:8]}"
        start_time = time.time()
        model = Config.NVIDIA_BUILDER_MODEL if iteration % 2 == 1 else Config.MINIMAX_MODEL
        prompt = self._construct_novelty_prompt(request, stack, iteration, prev_code, prev_attempts)

        build_log = f"[{datetime.now().isoformat()}] Novelty iteration {iteration} starting\n"
        try:
            code = await self.llm.generate_code(prompt, model, self.SYSTEM_PROMPT)
            if code.startswith("ERROR:"):
                success, error = False, code
                build_log += f"[{datetime.now().isoformat()}] FAILED: {error}\n"
            else:
                success, error = True, ""
                build_log += f"[{datetime.now().isoformat()}] SUCCESS: {len(code)} chars\n"
        except Exception as e:
            code, success, error = "", False, str(e)
            build_log += f"[{datetime.now().isoformat()}] EXCEPTION: {error}\n"

        return NoveltyAttempt(attempt_id=attempt_id, iteration=iteration, winning_config=stack,
                             code_artifact=code, build_log=build_log,
                             creativity_notes=self._creativity_notes(iteration, stack, success),
                             build_time_seconds=time.time() - start_time, success=success,
                             timestamp=datetime.now().isoformat())

    def _construct_novelty_prompt(self, request, stack, iteration, prev_code, prev_attempts):
        learnings = ""
        if prev_attempts:
            learnings = "\n## PREVIOUS LEARNINGS:\n" + "\n".join(
                f"Iteration {p.iteration}: {p.creativity_notes}" for p in prev_attempts)

        directions = {
            1: "Focus: Unexpected visual design, unique palettes, creative layouts. Add: Smooth animations, micro-interactions.",
            2: "Focus: Advanced interactivity, gamification, storytelling. Add: Physics-based animations, 3D elements.",
            3: "Focus: Pushing absolute boundaries. Add: Experimental features, artistic code expression."
        }

        return f"""# NOVELTY BUILD - Iteration {iteration}
## Request: {request.code_type.value} - {request.description}
## Stack: {stack.name}
{learnings}
## Directions: {directions.get(iteration, "Be creative")}
## Previous Code (do NOT copy):
```
{prev_code[:2000]}
```
Generate the MOST CREATIVE version. Make it unforgettable."""

    def _creativity_notes(self, iteration, stack, success):
        notes = [f"Iteration {iteration} approach:"]
        if iteration == 1: notes.extend(["Visual innovation and micro-interactions", "Unexpected color theory"])
        elif iteration == 2: notes.extend(["Gamification and advanced interactivity", "Storytelling in UX"])
        else: notes.extend(["Pushing creative boundaries", "Experimental features"])
        notes.extend([f"Stack: {stack.name}", f"Success: {success}"])
        return "\n".join(notes)


# ═══════════════════════════════════════════════════════════════════════════════
#  LEADERBOARD SYSTEM
# ═══════════════════════════════════════════════════════════════════════════════

class LeaderboardSystem:
    def __init__(self, db_path=None):
        self.db_path = db_path or Config.DB_PATH
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS leaderboard (
                entry_id TEXT PRIMARY KEY, project_name TEXT NOT NULL,
                code_type TEXT NOT NULL, score REAL NOT NULL,
                novelty_rating INTEGER NOT NULL, tool_stack TEXT NOT NULL,
                build_time_seconds REAL NOT NULL, user_rating INTEGER,
                created_at TEXT NOT NULL, download_path TEXT, model_used TEXT
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON leaderboard(created_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_score ON leaderboard(score DESC)")
        conn.commit()
        conn.close()

    def add_entry(self, entry):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO leaderboard (entry_id, project_name, code_type, score, novelty_rating,
            tool_stack, build_time_seconds, user_rating, created_at, download_path, model_used)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (entry.entry_id, entry.project_name, entry.code_type, entry.score,
              entry.novelty_rating, entry.tool_stack, entry.build_time_seconds,
              entry.user_rating, entry.created_at, entry.download_path, entry.model_used))
        conn.commit()
        conn.close()
        return entry.entry_id

    def get_entries(self, timeframe="all_time", code_type=None, sort_by="score", limit=50):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        now = datetime.now()
        if timeframe == "current_month":
            start_date = now.replace(day=1).isoformat()
        elif timeframe == "past_year":
            start_date = (now - timedelta(days=365)).isoformat()
        else:
            start_date = "1970-01-01"

        query = "SELECT * FROM leaderboard WHERE created_at >= ?"
        params = [start_date]
        if code_type:
            query += " AND code_type = ?"
            params.append(code_type)
        sort_column = "score" if sort_by == "score" else "created_at"
        query += f" ORDER BY {sort_column} DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()
        entries = []
        for row in rows:
            entries.append(LeaderboardEntry(
                entry_id=row["entry_id"], project_name=row["project_name"],
                code_type=row["code_type"], score=row["score"],
                novelty_rating=row["novelty_rating"], tool_stack=row["tool_stack"],
                build_time_seconds=row["build_time_seconds"], user_rating=row["user_rating"],
                created_at=row["created_at"], download_path=row["download_path"],
                model_used=row["model_used"]
            ))
        conn.close()
        return entries

    def get_stats(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM leaderboard")
        total = cursor.fetchone()[0]
        cursor.execute("SELECT AVG(score) FROM leaderboard")
        avg_score = cursor.fetchone()[0] or 0
        cursor.execute("SELECT MAX(score) FROM leaderboard")
        max_score = cursor.fetchone()[0] or 0
        cursor.execute("SELECT code_type, COUNT(*) FROM leaderboard GROUP BY code_type")
        by_type = {row[0]: row[1] for row in cursor.fetchall()}
        cursor.execute("SELECT tool_stack, COUNT(*) FROM leaderboard GROUP BY tool_stack ORDER BY COUNT(*) DESC LIMIT 5")
        top_stacks = [{"stack": row[0], "count": row[1]} for row in cursor.fetchall()]
        conn.close()
        return {"total_entries": total, "average_score": round(avg_score, 2),
                "highest_score": max_score, "by_type": by_type, "top_stacks": top_stacks}

    def rate_entry(self, entry_id, rating):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("UPDATE leaderboard SET user_rating = ? WHERE entry_id = ?", (rating, entry_id))
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return updated

# ═══════════════════════════════════════════════════════════════════════════════
#  DOWNLOAD MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class DownloadManager:
    def __init__(self, output_dir=None):
        self.output_dir = output_dir or Config.OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def create_package(self, project_name, code_artifact, tool_stack, build_request):
        package_id = f"{project_name}_{uuid.uuid4().hex[:8]}"
        package_dir = self.output_dir / package_id
        package_dir.mkdir(parents=True, exist_ok=True)

        files = self._parse_code_artifact(code_artifact)

        src_dir = package_dir / "src"
        src_dir.mkdir(exist_ok=True)
        for filename, content in files.items():
            file_path = src_dir / filename
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(content, encoding="utf-8")

        readme = self._generate_readme(project_name, tool_stack, build_request, files)
        (package_dir / "README.md").write_text(readme, encoding="utf-8")

        requirements = self._generate_requirements(tool_stack)
        (package_dir / "requirements.txt").write_text(requirements, encoding="utf-8")

        package_info = {
            "project_name": project_name, "generated_at": datetime.now().isoformat(),
            "tool_stack": tool_stack.dict(), "build_request": build_request.dict(),
            "files": list(files.keys())
        }
        (package_dir / "package.json").write_text(json.dumps(package_info, indent=2), encoding="utf-8")

        zip_path = self.output_dir / f"{package_id}.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for file_path in package_dir.rglob("*"):
                if file_path.is_file():
                    zf.write(file_path, file_path.relative_to(package_dir))

        shutil.rmtree(package_dir)
        return str(zip_path)

    def _parse_code_artifact(self, code_artifact):
        files = {}
        pattern = r'```(?:file:\s*)?([^\n]+)\n(.*?)```'
        matches = re.findall(pattern, code_artifact, re.DOTALL)
        if matches:
            for filename, content in matches:
                filename = filename.strip()
                if filename and content.strip():
                    files[filename] = content.strip()
        else:
            pattern2 = r'(?:^|\n)//?\s*FILE:\s*([^\n]+)\n(.*?)(?=\n//?\s*FILE:|$)'
            matches2 = re.findall(pattern2, code_artifact, re.DOTALL | re.IGNORECASE)
            if matches2:
                for filename, content in matches2:
                    filename = filename.strip()
                    if filename and content.strip():
                        files[filename] = content.strip()
            else:
                files["main.py"] = code_artifact
        return files

    def _generate_readme(self, project_name, tool_stack, build_request, files):
        file_list = "\n".join(files.keys())
        return f"""# {project_name}

## Generated by The Gardener

A multi-agent code generation platform that cultivates the best code.

## Project Info
- **Type**: {build_request.code_type.value}
- **Description**: {build_request.description}
- **Generated**: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Tool Stack
- **Frontend**: {', '.join(tool_stack.frontend)}
- **Backend**: {', '.join(tool_stack.backend)}
- **Database**: {', '.join(tool_stack.database)}
- **Styling**: {', '.join(tool_stack.styling)}
- **Utilities**: {', '.join(tool_stack.utilities)}

## Files
```
{file_list}
```

## Getting Started
```bash
pip install -r requirements.txt
python src/main.py
```

## Stack Justification
{tool_stack.justification}

---
*Generated by The Gardener Platform*
"""

    def _generate_requirements(self, tool_stack):
        requirements = []
        all_tools = str(tool_stack.backend + tool_stack.database + tool_stack.utilities + tool_stack.styling).lower()
        if "fastapi" in all_tools: requirements.extend(["fastapi>=0.104.0", "uvicorn[standard]>=0.24.0"])
        if "flask" in all_tools: requirements.extend(["flask>=3.0.0", "gunicorn>=21.0.0"])
        if "django" in all_tools: requirements.extend(["django>=5.0.0"])
        if "sqlalchemy" in all_tools or "postgres" in all_tools: requirements.extend(["sqlalchemy>=2.0.0", "psycopg2-binary>=2.9.0"])
        if "redis" in all_tools: requirements.extend(["redis>=5.0.0"])
        if "pydantic" in all_tools or "zod" in all_tools: requirements.extend(["pydantic>=2.5.0", "email-validator>=2.1.0"])
        if "pytest" in all_tools: requirements.extend(["pytest>=7.4.0", "pytest-asyncio>=0.21.0"])
        if "docker" in all_tools: requirements.append("docker>=6.1.0")
        if "openai" in all_tools or "langchain" in all_tools: requirements.extend(["openai>=1.0.0", "langchain>=0.1.0"])
        if "pandas" in all_tools: requirements.extend(["pandas>=2.1.0", "numpy>=1.26.0"])
        if "streamlit" in all_tools: requirements.extend(["streamlit>=1.28.0"])
        if "auth" in all_tools: requirements.extend(["PyJWT>=2.8.0", "passlib[bcrypt]>=1.7.0"])
        if "stripe" in all_tools: requirements.extend(["stripe>=7.0.0"])
        requirements.extend(["python-dotenv>=1.0.0", "httpx>=0.25.0", "jinja2>=3.1.0"])
        return "\n".join(sorted(set(requirements)))


# ═══════════════════════════════════════════════════════════════════════════════
#  PIPELINE ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════════

class PipelineOrchestrator:
    def __init__(self):
        self.llm = LLMClient()
        self.meta_builder = MetaBuilder(self.llm)
        self.builder = BuilderBot(self.llm)
        self.reviewer = LLMReviewer(self.llm)
        self.ranker = Ranker(self.llm)
        self.novelty_builder = NoveltySiteBuilder(self.llm)
        self.leaderboard = LeaderboardSystem()
        self.download_manager = DownloadManager()
        self.db = get_database()
        self.current_build_id = None
        self.progress = {}
        self.results = {}

    async def run_pipeline(self, request, build_id=None):
        build_id = build_id or f"pipeline_{uuid.uuid4().hex[:8]}"
        self.current_build_id = build_id
        self.progress[build_id] = {"status": "started", "phase": "meta_builder",
                                    "message": "The Gardener is selecting seeds...", "percent": 5}
        start_time = time.time()

        try:
            # PHASE 1: META-BUILDER
            self._update_progress(build_id, "meta_builder", "Planting 5 distinct tool combinations...", 10)
            tool_combinations = self.meta_builder.generate_tool_combinations(request.code_type.value, request.preferred_frameworks)

            # PHASE 2: BUILDER BOT (5 attempts)
            self._update_progress(build_id, "builder", "Builder Bots are constructing...", 20)
            build_tasks = [self.builder.build(request, stack, i+1) for i, stack in enumerate(tool_combinations)]
            build_attempts = await asyncio.gather(*build_tasks)

            successful = [a for a in build_attempts if a.success]
            if not successful:
                self._update_progress(build_id, "failed", "All build attempts failed", 100)
                return {"status": "failed", "error": "All builds failed"}

            # PHASE 3: REVIEWER
            self._update_progress(build_id, "reviewer", "Reviewer is analyzing code quality...", 40)
            reviews = await self.reviewer.review_all(request, build_attempts)

            # PHASE 4: RANKER
            self._update_progress(build_id, "ranker", "Ranker is scoring all builds...", 55)
            ranked_builds = await self.ranker.rank_all(build_attempts, reviews)

            winner = ranked_builds[0]
            winning_attempt = next((a for a in build_attempts if a.attempt_id == winner.attempt_id), None)
            if not winning_attempt:
                return {"status": "failed", "error": "Ranking failed"}

            # PHASE 5: NOVELTY BUILDER
            self._update_progress(build_id, "novelty", "Novelty Builder is creating magic...", 70)
            novelty_attempts = await self.novelty_builder.build_novelty_versions(request, winning_attempt.tool_stack, winning_attempt)

            successful_novelty = [a for a in novelty_attempts if a.success]
            if not successful_novelty:
                final_code = winning_attempt.code_artifact
            else:
                final_code = successful_novelty[-1].code_artifact

            # PHASE 6: DOWNLOAD
            self._update_progress(build_id, "packaging", "Packaging final deliverable...", 85)
            project_name = f"{request.code_type.value}_project"
            zip_path = self.download_manager.create_package(project_name, final_code, winning_attempt.tool_stack, request)

            # PHASE 7: LEADERBOARD
            self._update_progress(build_id, "leaderboard", "Adding to leaderboard...", 95)
            total_time = time.time() - start_time

            entry = LeaderboardEntry(
                entry_id=build_id, project_name=project_name, code_type=request.code_type.value,
                score=winner.total_score, novelty_rating=winner.novelty_score,
                tool_stack=winning_attempt.tool_stack.name, build_time_seconds=total_time,
                user_rating=None, created_at=datetime.now().isoformat(),
                download_path=zip_path, model_used=winning_attempt.model_used
            )
            self.leaderboard.add_entry(entry)

            self._update_progress(build_id, "complete", "Build complete! Ready for download.", 100)

            results = {
                "status": "success", "build_id": build_id, "request": request.dict(),
                "tool_combinations": [t.dict() for t in tool_combinations],
                "build_attempts": [a.dict() for a in build_attempts],
                "reviews": [r.dict() for r in reviews],
                "ranked_builds": [r.dict() for r in ranked_builds],
                "winner": winner.dict(),
                "novelty_attempts": [a.dict() for a in novelty_attempts],
                "final_code": final_code, "download_path": zip_path,
                "total_time_seconds": total_time, "leaderboard_entry": entry.dict()
            }
            self.results[build_id] = results
            return results

        except Exception as e:
            self._update_progress(build_id, "failed", f"Pipeline failed: {str(e)}", 100)
            return {"status": "failed", "error": str(e)}

    def _update_progress(self, build_id, phase, message, percent):
        self.progress[build_id] = {"status": "running" if percent < 100 else "complete",
                                    "phase": phase, "message": message, "percent": percent,
                                    "timestamp": datetime.now().isoformat()}

    def get_progress(self, build_id):
        return self.progress.get(build_id, {"status": "unknown", "phase": "unknown",
                                            "message": "Build not found", "percent": 0})


# ═══════════════════════════════════════════════════════════════════════════════
#  FASTAPI APPLICATION
# ═══════════════════════════════════════════════════════════════════════════════

orchestrator = PipelineOrchestrator()

app = FastAPI(
    title="The Gardener - Multi-Agent Code Generation Platform",
    description="A meta-application that creates specialized AI agents for code generation, review, and ranking.",
    version="2.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ═══════════════════════════════════════════════════════════════════════════════
#  HTML TEMPLATES (Written to files for clean deployment)
# ═══════════════════════════════════════════════════════════════════════════════

def write_templates():
    index_html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>The Gardener - Code Generation Platform</title>
<style>
:root{--primary:#2d5016;--primary-light:#4a7c2e;--accent:#e8f5e9;--dark:#1a1a2e;--light:#f5f5f0;--border:#c8e6c9;--success:#4caf50;--warning:#ff9800;--error:#f44336;--gradient:linear-gradient(135deg,#2d5016 0%,#4a7c2e 50%,#66bb6a 100%);}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',system-ui,sans-serif;background:var(--light);color:var(--dark);line-height:1.6}
.hero{background:var(--gradient);color:white;padding:4rem 2rem;text-align:center;position:relative;overflow:hidden}
.hero h1{font-size:3.5rem;margin-bottom:1rem;text-shadow:2px 2px 4px rgba(0,0,0,0.2)}
.hero p{font-size:1.3rem;opacity:0.9;max-width:600px;margin:0 auto}
.container{max-width:1200px;margin:0 auto;padding:2rem}
.card{background:white;border-radius:16px;padding:2rem;margin-bottom:2rem;box-shadow:0 4px 20px rgba(0,0,0,0.08);border:1px solid var(--border)}
.card h2{color:var(--primary);margin-bottom:1.5rem;display:flex;align-items:center;gap:0.5rem}
.form-group{margin-bottom:1.5rem}
label{display:block;font-weight:600;margin-bottom:0.5rem;color:var(--primary)}
input,select,textarea{width:100%;padding:0.875rem 1rem;border:2px solid var(--border);border-radius:10px;font-size:1rem;transition:all 0.3s;background:white}
input:focus,select:focus,textarea:focus{outline:none;border-color:var(--primary-light);box-shadow:0 0 0 3px rgba(45,80,22,0.1)}
textarea{min-height:120px;resize:vertical}
.btn{display:inline-flex;align-items:center;gap:0.5rem;padding:1rem 2rem;border:none;border-radius:10px;font-size:1.1rem;font-weight:600;cursor:pointer;transition:all 0.3s;text-decoration:none}
.btn-primary{background:var(--gradient);color:white;box-shadow:0 4px 15px rgba(45,80,22,0.3)}
.btn-primary:hover{transform:translateY(-2px);box-shadow:0 6px 20px rgba(45,80,22,0.4)}
.progress-container{display:none;margin-top:2rem}
.progress-container.active{display:block}
.progress-bar{height:12px;background:var(--accent);border-radius:6px;overflow:hidden;margin-bottom:1rem}
.progress-fill{height:100%;background:var(--gradient);border-radius:6px;transition:width 0.5s ease;width:0%}
.progress-message{text-align:center;font-weight:600;color:var(--primary);font-size:1.1rem}
.pipeline-visual{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:1rem;margin:2rem 0}
.pipeline-step{background:var(--accent);padding:1.5rem;border-radius:12px;text-align:center;border:2px solid var(--border);transition:all 0.3s}
.pipeline-step.active{background:var(--primary);color:white;border-color:var(--primary-light);transform:scale(1.05)}
.pipeline-step .icon{font-size:2rem;margin-bottom:0.5rem}
.stats-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:1.5rem;margin:2rem 0}
.stat-card{background:white;padding:1.5rem;border-radius:12px;border-left:4px solid var(--primary);box-shadow:0 2px 10px rgba(0,0,0,0.05)}
.stat-value{font-size:2rem;font-weight:700;color:var(--primary)}
.stat-label{color:#666;font-size:0.9rem}
table{width:100%;border-collapse:collapse;margin:1rem 0}
th,td{padding:1rem;text-align:left;border-bottom:1px solid var(--border)}
th{background:var(--accent);color:var(--primary);font-weight:600}
tr:hover{background:rgba(232,245,233,0.5)}
.badge{display:inline-block;padding:0.25rem 0.75rem;border-radius:20px;font-size:0.85rem;font-weight:600}
.badge-success{background:rgba(76,175,80,0.2);color:var(--success)}
.badge-warning{background:rgba(255,152,0,0.2);color:var(--warning)}
.nav{background:white;padding:1rem 2rem;box-shadow:0 2px 10px rgba(0,0,0,0.05);position:sticky;top:0;z-index:100}
.nav-content{max-width:1200px;margin:0 auto;display:flex;justify-content:space-between;align-items:center}
.nav-links{display:flex;gap:2rem}
.nav-links a{color:var(--primary);text-decoration:none;font-weight:600;transition:color 0.3s}
.nav-links a:hover{color:var(--primary-light)}
.result-section{display:none}
.result-section.active{display:block}
.code-preview{background:#1a1a2e;color:#a6e3a1;padding:1.5rem;border-radius:12px;overflow-x:auto;font-family:'Fira Code','Consolas',monospace;font-size:0.9rem;line-height:1.5;max-height:500px;overflow-y:auto}
.download-btn{display:inline-flex;align-items:center;gap:0.5rem;padding:1rem 2rem;background:var(--success);color:white;border-radius:10px;text-decoration:none;font-weight:600;margin-top:1rem;transition:all 0.3s}
.download-btn:hover{transform:translateY(-2px);box-shadow:0 4px 15px rgba(76,175,80,0.3)}
@media(max-width:768px){.hero h1{font-size:2.5rem}.pipeline-visual{grid-template-columns:1fr}.stats-grid{grid-template-columns:1fr}}
</style>
</head>
<body>
<nav class="nav"><div class="nav-content"><div style="font-size:1.5rem;font-weight:700;color:var(--primary)">The Gardener</div><div class="nav-links"><a href="/">Home</a><a href="/leaderboard">Leaderboard</a><a href="/settings">Settings</a><a href="/docs">API Docs</a></div></div></nav>
<div class="hero"><h1>The Gardener</h1><p>Multi-Agent Code Generation & Curation Platform<br>Where AI agents plant, grow, and harvest the finest code</p></div>
<div class="container">
<div class="card" id="buildForm"><h2>Start a New Build</h2>
<form id="buildFormElement" action="/api/build" method="POST">
<div class="form-group"><label for="code_type">Project Type</label><select name="code_type" id="code_type" required>
<option value="website">Website</option><option value="web_app">Web Application</option>
<option value="api_backend">API Backend</option><option value="cli_tool">CLI Tool</option>
<option value="data_pipeline">Data Pipeline</option><option value="game">Game</option>
<option value="mobile_app">Mobile App</option><option value="chatbot">Chatbot</option>
<option value="dashboard">Dashboard</option><option value="e_commerce">E-Commerce</option>
<option value="portfolio">Portfolio</option><option value="blog">Blog</option>
<option value="custom">Custom</option></select></div>
<div class="form-group"><label for="description">Project Description</label><textarea name="description" id="description" placeholder="Describe what you want to build. Be specific about features, target users, and goals." required></textarea></div>
<div class="form-group"><label for="specific_requirements">Specific Requirements (Optional)</label><textarea name="specific_requirements" id="specific_requirements" placeholder="Any specific features, integrations, or constraints..."></textarea></div>
<div class="form-group"><label for="target_audience">Target Audience (Optional)</label><input type="text" name="target_audience" id="target_audience" placeholder="e.g., Small businesses, Developers"></div>
<div class="form-group"><label for="complexity_level">Complexity Level</label><select name="complexity_level" id="complexity_level">
<option value="simple">Simple - MVP/Prototype</option><option value="medium" selected>Medium - Production Ready</option>
<option value="complex">Complex - Enterprise Grade</option></select></div>
<button type="submit" class="btn btn-primary">Plant & Grow Code</button></form></div>
<div class="card progress-container" id="progressContainer"><h2>Build Progress</h2>
<div class="progress-bar"><div class="progress-fill" id="progressFill"></div></div>
<div class="progress-message" id="progressMessage">Initializing...</div>
<div class="pipeline-visual" id="pipelineVisual">
<div class="pipeline-step" data-phase="meta_builder"><div class="icon">&#127793;</div><div>Meta-Builder</div><small>Planting seeds</small></div>
<div class="pipeline-step" data-phase="builder"><div class="icon">&#128296;</div><div>Builder Bot</div><small>5 attempts</small></div>
<div class="pipeline-step" data-phase="reviewer"><div class="icon">&#128269;</div><div>Reviewer</div><small>Analysis</small></div>
<div class="pipeline-step" data-phase="ranker"><div class="icon">&#128202;</div><div>Ranker</div><small>Scoring</small></div>
<div class="pipeline-step" data-phase="novelty"><div class="icon">&#10024;</div><div>Novelty Builder</div><small>3 iterations</small></div>
<div class="pipeline-step" data-phase="complete"><div class="icon">&#127873;</div><div>Complete</div><small>Ready</small></div>
</div></div>
<div class="card result-section" id="resultsSection"><h2>Build Complete!</h2><div id="resultsContent"></div></div>
<div class="card"><h2>Recent Builds</h2><div id="leaderboardPreview"><p>Loading leaderboard...</p></div>
<a href="/leaderboard" class="btn btn-primary" style="margin-top:1rem">View Full Leaderboard</a></div>
</div>
<script>
let buildId=null,pollInterval=null;
document.getElementById('buildFormElement').addEventListener('submit',async(e)=>{
e.preventDefault();
const data=Object.fromEntries(new FormData(e.target));
document.getElementById('progressContainer').classList.add('active');
document.getElementById('resultsSection').classList.remove('active');
try{
const res=await fetch('/api/build',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
if(!res.ok){const err=await res.json().catch(()=>({}));showError(err.detail||err.error||'Build failed ('+res.status+')');return;}
const result=await res.json();
if(result.build_id){buildId=result.build_id;startPolling();}
else showError(result.detail||result.error||'Build failed');
}catch(err){showError('Network error: '+err.message);}
});
function startPolling(){if(pollInterval)clearInterval(pollInterval);pollInterval=setInterval(async()=>{try{const res=await fetch('/api/progress/'+buildId);if(!res.ok){const err=await res.json().catch(()=>({}));if(res.status===404)showError(err.detail||'Build not found');return;}const p=await res.json();updateProgress(p);if(p.status==='complete'||p.status==='failed'){clearInterval(pollInterval);if(p.status==='complete')loadResults(buildId);}}catch(err){console.error(err);}},2000);}
function updateProgress(p){document.getElementById('progressFill').style.width=p.percent+'%';document.getElementById('progressMessage').textContent=p.message;document.querySelectorAll('.pipeline-step').forEach(s=>{s.classList.remove('active');if(s.dataset.phase===p.phase)s.classList.add('active');});}
async function loadResults(id){try{const res=await fetch('/api/results/'+id);if(!res.ok){const err=await res.json().catch(()=>({}));showError(err.detail||'Results not ready');return;}const r=await res.json();if(r.status==='success'||r.winner)displayResults(r);else showError(r.detail||r.error||'No results available');}catch(err){showError('Failed to load results');}}
function displayResults(r){const c=document.getElementById('resultsContent');c.innerHTML=`<div class="stats-grid"><div class="stat-card"><div class="stat-value">${r.winner.total_score}</div><div class="stat-label">Final Score / 100</div></div><div class="stat-card"><div class="stat-value">${r.winner.tool_stack_name}</div><div class="stat-label">Winning Stack</div></div><div class="stat-card"><div class="stat-value">${r.total_time_seconds.toFixed(1)}s</div><div class="stat-label">Build Time</div></div><div class="stat-card"><div class="stat-value">${r.novelty_attempts.filter(a=>a.success).length}/3</div><div class="stat-label">Novelty Iterations</div></div></div><h3>Rankings</h3><table><thead><tr><th>Rank</th><th>Stack</th><th>Score</th><th>Functionality</th><th>Quality</th><th>Novelty</th></tr></thead><tbody>${r.ranked_builds.map(b=>`<tr><td><strong>#${b.rank}</strong></td><td>${b.tool_stack_name}</td><td><span class="badge ${b.total_score>=80?'badge-success':b.total_score>=60?'badge-warning':''}">${b.total_score}</span></td><td>${b.functionality_score}</td><td>${b.code_quality_score}</td><td>${b.novelty_score}</td></tr>`).join('')}</tbody></table><h3>Generated Code Preview</h3><div class="code-preview">${r.final_code.substring(0,2000).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}...</div><a href="/api/download/${buildId}" class="download-btn">Download Complete Package</a>`;document.getElementById('resultsSection').classList.add('active');loadLeaderboardPreview();}
function showError(m){if(!m||m==='undefined')m='An unknown error occurred';document.getElementById('progressMessage').textContent='ERROR: '+m;document.getElementById('progressFill').style.width='100%';document.getElementById('progressFill').style.background='var(--error)';}
async function loadLeaderboardPreview(){try{const res=await fetch('/api/leaderboard?limit=5');const d=await res.json();const el=document.getElementById('leaderboardPreview');if(d.entries&&d.entries.length){el.innerHTML=`<table><thead><tr><th>Project</th><th>Type</th><th>Score</th><th>Stack</th><th>Date</th></tr></thead><tbody>${d.entries.map(e=>`<tr><td>${e.project_name}</td><td>${e.code_type}</td><td><span class="badge badge-success">${e.score}</span></td><td>${e.tool_stack}</td><td>${new Date(e.created_at).toLocaleDateString()}</td></tr>`).join('')}</tbody></table>`;}else{el.innerHTML='<p>No builds yet. Be the first!</p>';}}catch(err){console.error(err);}}
loadLeaderboardPreview();
</script>
</body>
</html>"""

    leaderboard_html = """<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Leaderboard - The Gardener</title>
<style>
:root{--primary:#2d5016;--primary-light:#4a7c2e;--accent:#e8f5e9;--dark:#1a1a2e;--light:#f5f5f0;--border:#c8e6c9;--success:#4caf50;--warning:#ff9800;}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',system-ui,sans-serif;background:var(--light);color:var(--dark)}
.nav{background:white;padding:1rem 2rem;box-shadow:0 2px 10px rgba(0,0,0,0.05)}
.nav-content{max-width:1200px;margin:0 auto;display:flex;justify-content:space-between;align-items:center}
.nav-links a{color:var(--primary);text-decoration:none;font-weight:600;margin-left:2rem}
.container{max-width:1200px;margin:0 auto;padding:2rem}
.hero{background:linear-gradient(135deg,#2d5016 0%,#4a7c2e 100%);color:white;padding:3rem 2rem;text-align:center;margin-bottom:2rem}
.hero h1{font-size:2.5rem;margin-bottom:0.5rem}
.filters{display:flex;gap:1rem;margin-bottom:2rem;flex-wrap:wrap}
.filters select,.filters button{padding:0.75rem 1rem;border:2px solid var(--border);border-radius:8px;font-size:1rem;background:white;cursor:pointer}
.filters button{background:var(--primary);color:white;border-color:var(--primary)}
.stats-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:1.5rem;margin-bottom:2rem}
.stat-card{background:white;padding:1.5rem;border-radius:12px;box-shadow:0 2px 10px rgba(0,0,0,0.05);text-align:center}
.stat-value{font-size:2.5rem;font-weight:700;color:var(--primary)}
table{width:100%;background:white;border-radius:12px;overflow:hidden;box-shadow:0 2px 10px rgba(0,0,0,0.05)}
th,td{padding:1rem;text-align:left;border-bottom:1px solid var(--border)}
th{background:var(--accent);color:var(--primary);font-weight:600}
tr:hover{background:rgba(232,245,233,0.5)}
.badge{display:inline-block;padding:0.25rem 0.75rem;border-radius:20px;font-size:0.85rem;font-weight:600}
.badge-gold{background:#ffd700;color:#333}
.badge-silver{background:#c0c0c0;color:#333}
.badge-bronze{background:#cd7f32;color:white}
.badge-success{background:rgba(76,175,80,0.2);color:var(--success)}
</style></head>
<body>
<nav class="nav"><div class="nav-content"><div style="font-size:1.5rem;font-weight:700;color:var(--primary)">The Gardener</div><div class="nav-links"><a href="/">Home</a><a href="/leaderboard">Leaderboard</a><a href="/settings">Settings</a><a href="/docs">API Docs</a></div></div></nav>
<div class="hero"><h1>Build Leaderboard</h1><p>See how our AI agents' creations rank across time</p></div>
<div class="container">
<div class="stats-grid" id="statsGrid">
<div class="stat-card"><div class="stat-value" id="totalEntries">-</div><div>Total Builds</div></div>
<div class="stat-card"><div class="stat-value" id="avgScore">-</div><div>Average Score</div></div>
<div class="stat-card"><div class="stat-value" id="highestScore">-</div><div>Highest Score</div></div>
</div>
<div class="filters">
<select id="timeframe"><option value="all_time">All Time</option><option value="current_month">This Month</option><option value="past_year">Past Year</option></select>
<select id="codeType"><option value="">All Types</option><option value="website">Website</option><option value="web_app">Web App</option><option value="api_backend">API Backend</option><option value="dashboard">Dashboard</option><option value="chatbot">Chatbot</option><option value="game">Game</option></select>
<select id="sortBy"><option value="score">By Score</option><option value="created_at">By Date</option></select>
<button onclick="loadLeaderboard()">Refresh</button>
</div>
<table><thead><tr><th>Rank</th><th>Project</th><th>Type</th><th>Score</th><th>Novelty</th><th>Stack</th><th>Build Time</th><th>Date</th><th>Download</th></tr></thead>
<tbody id="leaderboardBody"><tr><td colspan="9" style="text-align:center">Loading...</td></tr></tbody>
</table></div>
<script>
async function loadLeaderboard(){const tf=document.getElementById('timeframe').value,ct=document.getElementById('codeType').value,sb=document.getElementById('sortBy').value;const params=new URLSearchParams({timeframe:tf,sort_by:sb,limit:'50'});if(ct)params.append('code_type',ct);try{const res=await fetch('/api/leaderboard?'+params);const d=await res.json();if(d.stats){document.getElementById('totalEntries').textContent=d.stats.total_entries;document.getElementById('avgScore').textContent=d.stats.average_score;document.getElementById('highestScore').textContent=d.stats.highest_score;}const tb=document.getElementById('leaderboardBody');if(d.entries&&d.entries.length){tb.innerHTML=d.entries.map((e,i)=>`<tr><td>${i===0?'<span class="badge badge-gold">1st</span>':i===1?'<span class="badge badge-silver">2nd</span>':i===2?'<span class="badge badge-bronze">3rd</span>':'#'+(i+1)}</td><td><strong>${e.project_name}</strong></td><td>${e.code_type}</td><td><span class="badge badge-success">${e.score}</span></td><td>${e.novelty_rating}/100</td><td>${e.tool_stack}</td><td>${e.build_time_seconds.toFixed(1)}s</td><td>${new Date(e.created_at).toLocaleDateString()}</td><td>${e.download_path?'<a href="/api/download-file?path='+encodeURIComponent(e.download_path)+'" style="color:var(--primary);text-decoration:none">Download</a>':'-'}</td></tr>`).join('');}else{tb.innerHTML='<tr><td colspan="9" style="text-align:center;padding:2rem">No entries yet. Start building!</td></tr>';}}catch(err){console.error(err);}}
loadLeaderboard();
</script></body></html>"""

    (Config.STATIC_DIR / "index.html").write_text(index_html, encoding="utf-8")
    (Config.STATIC_DIR / "leaderboard.html").write_text(leaderboard_html, encoding="utf-8")
    return index_html, leaderboard_html

# Write templates on startup
INDEX_HTML, LEADERBOARD_HTML = write_templates()


# ═══════════════════════════════════════════════════════════════════════════════
#  API ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
async def root():
    index_path = Config.STATIC_DIR / "index.html"
    if index_path.exists():
        return HTMLResponse(content=index_path.read_text(encoding="utf-8"))
    return HTMLResponse(content=INDEX_HTML)

@app.get("/leaderboard", response_class=HTMLResponse)
async def leaderboard_page():
    leaderboard_path = Config.STATIC_DIR / "leaderboard.html"
    if leaderboard_path.exists():
        return HTMLResponse(content=leaderboard_path.read_text(encoding="utf-8"))
    return HTMLResponse(content=LEADERBOARD_HTML)

@app.post("/api/build")
async def start_build(request: BuildRequest, background_tasks: BackgroundTasks):
    if not Config.NVIDIA_API_KEY and not Config.OPENROUTER_API_KEY:
        raise HTTPException(status_code=400, detail="No API keys configured. Set NVIDIA_API_KEY and/or OPENROUTER_API_KEY in environment.")
    build_id = f"pipeline_{uuid.uuid4().hex[:8]}"
    background_tasks.add_task(orchestrator.run_pipeline, request, build_id)
    return {"build_id": build_id, "status": "started", "message": "Build pipeline initiated"}

@app.get("/api/progress/{build_id}")
async def get_progress(build_id: str):
    return orchestrator.get_progress(build_id)

@app.get("/api/results/{build_id}")
async def get_results(build_id: str):
    results = orchestrator.db.get_results(build_id)
    if not results:
        raise HTTPException(status_code=404, detail="Build not found or not complete")
    return results

@app.get("/api/leaderboard")
async def get_leaderboard(timeframe: str = "all_time", code_type: str = None, sort_by: str = "score", limit: int = 50):
    entries = orchestrator.leaderboard.get_entries(timeframe, code_type, sort_by, limit)
    stats = orchestrator.leaderboard.get_stats()
    return {"entries": [e.model_dump() for e in entries], "stats": stats}

@app.post("/api/leaderboard/rate/{entry_id}")
async def rate_entry(entry_id: str, rating: int = Form(...)):
    if not 1 <= rating <= 5:
        raise HTTPException(status_code=400, detail="Rating must be 1-5")
    if not orchestrator.leaderboard.rate_entry(entry_id, rating):
        raise HTTPException(status_code=404, detail="Entry not found")
    return {"status": "success", "message": "Rating added"}

@app.get("/api/download/{build_id}")
async def download_build(build_id: str):
    results = orchestrator.db.get_results(build_id)
    if not results or not results.get("download_path"):
        raise HTTPException(status_code=404, detail="Build or download not found")
    zip_path = results["download_path"]
    if not os.path.exists(zip_path):
        raise HTTPException(status_code=404, detail="File not found on disk")
    return FileResponse(zip_path, media_type="application/zip", filename=f"gardener_build_{build_id}.zip")

@app.get("/api/download-file")
async def download_file(path: str):
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, media_type="application/zip", filename=os.path.basename(path))

@app.get("/api/stats")
async def get_stats():
    return orchestrator.leaderboard.get_stats()

@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy", "version": "2.0.0",
        "nvidia_api": "configured" if Config.NVIDIA_API_KEY else "not configured",
        "openrouter_api": "configured" if Config.OPENROUTER_API_KEY else "not configured"
    }

# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════



# ═══════════════════════════════════════════════════════════════════════════════
#  SETTINGS & CONFIG API
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/settings", response_class=HTMLResponse)
async def settings_page():
    settings_path = Config.STATIC_DIR / "settings.html"
    if settings_path.exists():
        return HTMLResponse(content=settings_path.read_text())
    return HTMLResponse(content="<h1>Settings</h1><p>Please restart the server to generate the settings page.</p>")

@app.get("/api/config")
async def get_config():
    # Return public view of config plus stored API key lists (non-secret keys are returned masked)
    cfg = Config.to_public_dict()
    keys = api_keys.get_all()
    # mask keys lightly for UI list display
    def mask_list(lst):
        return ["" if not k else (k[:6] + "..." + k[-4:]) for k in lst]
    cfg["api_keys"] = {"nvidia": mask_list(keys.get("nvidia", [])), "openrouter": mask_list(keys.get("openrouter", [])), "openrouter_paid": mask_list(keys.get("openrouter_paid", []))}
    return cfg

@app.post("/api/config")
async def update_config(request: Request):
    data = await request.json()
    # Update runtime config values
    try:
        Config.update_from_settings(data)
    except Exception:
        pass
    # Handle API keys arrays (optional)
    keys = data.get("api_keys") or {}
    if isinstance(keys, dict):
        # replace buckets if provided
        for provider in ("nvidia", "openrouter", "openrouter_paid"):
            vals = keys.get(provider)
            if isinstance(vals, list):
                # overwrite current list
                # write via core.api_keys
                current = api_keys.get_all()
                current[provider] = [v for v in vals if v]
                api_keys.write_keys(current)
    return {"status": "success", "message": "Configuration saved"}

@app.post("/api/config/test")
async def test_provider(request: Request):
    data = await request.json()
    provider = data.get("provider", "ollama")
    model = data.get("model", None)
    llm = LLMClient()
    success, message = await llm.test_connection(provider, model)
    return {"success": success, "message": message}

@app.get("/api/models")
async def list_models():
    return {
        "examples": {
            "openrouter_free": [
                "deepseek/deepseek-r1:free",
                "meta-llama/llama-4-maverick:free",
                "google/gemini-2.5-pro-exp-03-25:free",
                "nvidia/llama-3.1-nemotron-70b-instruct:free"
            ],
            "openrouter_paid": [
                "anthropic/claude-3.5-sonnet",
                "openai/gpt-4o",
                "meta-llama/llama-4-scout"
            ],
            "ollama_local": [
                "llama3.2",
                "llama3.1",
                "mistral",
                "codellama",
                "phi3",
                "qwen2.5",
                "deepseek-coder"
            ],
            "nvidia": [
                "nvidia/nemotron-3-super",
                "nvidia/llama-3.1-nemotron-70b-instruct"
            ]
        },
        "message": "You can enter ANY model name. These are just examples."
    }

if __name__ == "__main__":
    import uvicorn
    print("""
    ================================================================
       THE GARDENER - Multi-Agent Code Generation Platform
    ================================================================
    Architecture:
      1. Meta-Builder (The Gardener) - Orchestrates tool selection
      2. Builder Bot (5 attempts) - Generates functional code
      3. LLM Reviewer - Detailed critique & analysis
      4. Ranker - 0-100 scoring with weighted criteria
      5. Novelty Builder (3 iterations) - Maximizes creativity
      6. Leaderboard - SQLite persistence & analytics
      7. Download Manager - ZIP packaging with README

    API Integration:
      - NVIDIA API (Nemotron 3 Super) - Primary builder & reviewer
      - MiniMax M2.5 (OpenRouter) - Secondary builder
      - OpenRouter Free Tier (Llama 4 Maverick) - Ranker
    ================================================================
    """)
    if not Config.NVIDIA_API_KEY:
        print("Warning: NVIDIA_API_KEY not set. Set in .env or environment.")
    if not Config.OPENROUTER_API_KEY:
        print("Warning: OPENROUTER_API_KEY not set. Set in .env or environment.")
    print("\nStarting server at http://localhost:8000")
    print("API Docs: http://localhost:8000/docs")
    print("Leaderboard: http://localhost:8000/leaderboard\n")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
