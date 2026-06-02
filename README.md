# CodeForge — AI Code Generation Platform

Multi-agent code generation system with builder, reviewer, ranker, and novelty engine.

## Architecture

```
orchestrator/     → The Gardener (meta-builder, tool planner)
builder/          → 5-Attempt Code Builder (GLM-5.1 + MiniMax)
reviewer/         → LLM Code Reviewer (GLM-5.1)
ranker/           → Meta-Ranker (OpenRouter Free Model)
novelty_builder/  → 3-Attempt Novelty Engine
leaderboard/      → SQLite rankings (monthly/yearly/all-time)
frontend/         → Flask web UI
shared/           → Database + LLM client
config/           → Settings + tool inventory
```

## Quick Start (Local)

```bash
cd n4mint_codeforge
cp .env.example .env
# Edit .env with your API keys
pip install -r requirements.txt
python run.py
# Open http://localhost:5000
```

## Docker Deployment (justrunmy.app)

### Build & Run Locally
```bash
cd n4mint_codeforge
cp .env.example .env
# Edit .env with your API keys
docker-compose up --build
# Open http://localhost:8080
```

### Deploy to justrunmy.app

1. **Push to your repo** (GitHub/GitLab)
2. **Connect justrunmy.app** to your repo
3. **Set environment variables** in justrunmy.app dashboard:
   - `NVIDIA_API_KEY`
   - `OPENROUTER_API_KEY`
4. **Deploy** — it auto-detects the Dockerfile

### Manual Docker Build
```bash
docker build -t codeforge .
docker run -p 8080:8080   -e NVIDIA_API_KEY=your_key   -e OPENROUTER_API_KEY=your_key   codeforge
```

## API Stack

- **NVIDIA API**: GLM-5.1 for building and reviewing
- **OpenRouter**: MiniMax M2.1/M2.5 for variations + Free Ranker for scoring
- **OpenRouter Free Tier**: Auto-selects best available free model for ranking

## Pipeline Flow

1. **Gardener** analyzes request → plants 5 tool configurations
2. **Builder** runs 5 attempts with different tool stacks
3. **Reviewer** critiques all 5, provides comparative analysis
4. **Meta-Ranker** scores each 0-100, picks winner
5. **Novelty Engine** builds 3 creative versions of winning config
6. **Leaderboard** tracks all builds, enables downloads

## Downloading Code

All builds from the Novelty phase are downloadable as ZIP files containing:
- All source files
- `_codeforge_meta.json` with build metadata

## Folder Safety

Each module is self-contained. If the orchestrator crashes:
- `builder/` can run standalone with manual tool configs
- `reviewer/` can review any code artifacts
- `ranker/` can score any set of builds
- `leaderboard/` persists all data in SQLite


## Deployment Options

See `deploy_configs/` for pre-configured deployment files for 7 platforms:
- **Render** — Easiest, Heroku-like
- **Koyeb** — Always on, no cold starts
- **Fly.io** — Global edge, Docker-native
- **SnapDeploy** — Zero config, auto-wake
- **Google Cloud Run** — Production serverless
- **Zeabur** — Fast deploy, good DX
- **Railway** — Best developer experience

Quick deploy: pick a platform, copy the config file to root, push to GitHub, connect repo.
