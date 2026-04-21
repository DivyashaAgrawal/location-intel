#!/usr/bin/env bash
# Location Intelligence - one-shot setup.
#
# What it does:
#   1. Verifies Python 3.10+
#   2. Creates venv/ if missing and installs the package with dev extras
#   3. Installs the Ollama daemon if not already on PATH
#       - macOS: `brew install ollama`
#       - Linux/WSL: curl installer from ollama.com
#   4. Pulls the default model (llama3.2:3b)
#   5. Installs Redis and starts it (hot query cache)
#       - macOS: `brew install redis` + `brew services start redis`
#       - Linux: apt/dnf/yum install + `systemctl start` (falls back to
#         `redis-server --daemonize yes`)
#   6. Copies .env.example to .env if .env is missing
#   7. Initialises the SQLite DB
#
# Safe to re-run. Skip flags: --skip-ollama, --skip-model, --skip-redis.

set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
cd "$REPO_ROOT"

# ---------- args ----------
SKIP_OLLAMA=0
SKIP_MODEL=0
SKIP_REDIS=0
MODEL="${OLLAMA_MODEL:-llama3.2:3b}"

for arg in "$@"; do
    case "$arg" in
        --skip-ollama) SKIP_OLLAMA=1 ;;
        --skip-model)  SKIP_MODEL=1 ;;
        --skip-redis)  SKIP_REDIS=1 ;;
        --model=*)     MODEL="${arg#*=}" ;;
        -h|--help)
            grep '^#' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "Unknown arg: $arg" >&2; exit 2 ;;
    esac
done

# ---------- helpers ----------
info()  { printf '\033[1;34m[setup]\033[0m %s\n' "$*"; }
warn()  { printf '\033[1;33m[warn]\033[0m  %s\n' "$*" >&2; }
error() { printf '\033[1;31m[err]\033[0m   %s\n' "$*" >&2; }

have() { command -v "$1" >/dev/null 2>&1; }

# ---------- 1. Python ----------
info "Checking Python..."
if ! have python3; then
    error "python3 not found. Install Python 3.10+ and retry."
    exit 1
fi
PY_VER=$(python3 -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')
PY_MAJOR=$(python3 -c 'import sys; print(sys.version_info[0])')
PY_MINOR=$(python3 -c 'import sys; print(sys.version_info[1])')
if [[ "$PY_MAJOR" -lt 3 ]] || { [[ "$PY_MAJOR" -eq 3 ]] && [[ "$PY_MINOR" -lt 10 ]]; }; then
    error "Python $PY_VER detected; need >=3.10."
    exit 1
fi
info "Python $PY_VER detected."

# ---------- 2. venv + pip install ----------
if [[ ! -d venv ]]; then
    info "Creating venv/"
    python3 -m venv venv
fi
# shellcheck disable=SC1091
source venv/bin/activate
info "Upgrading pip..."
python -m pip install --quiet --upgrade pip
info "Installing package (editable, with dev extras)..."
python -m pip install --quiet -e '.[dev]'

# ---------- 3. Ollama daemon ----------
install_ollama() {
    if have ollama; then
        info "Ollama already installed: $(ollama --version 2>/dev/null || echo 'version unknown')"
        return 0
    fi

    local kernel
    kernel="$(uname -s)"
    case "$kernel" in
        Darwin)
            if have brew; then
                info "Installing Ollama via Homebrew..."
                brew install ollama
            else
                warn "Homebrew not found. Install manually from https://ollama.com/download and rerun."
                return 1
            fi
            ;;
        Linux)
            info "Installing Ollama via the official script..."
            curl -fsSL https://ollama.com/install.sh | sh
            ;;
        MINGW*|MSYS*|CYGWIN*)
            warn "Detected Windows shell. Install Ollama from https://ollama.com/download/windows and rerun."
            return 1
            ;;
        *)
            warn "Unknown platform $kernel. Install Ollama manually from https://ollama.com/download."
            return 1
            ;;
    esac
}

pull_model() {
    info "Pulling Ollama model: $MODEL"
    # Don't spin up a foreground server -- `ollama pull` will talk to it
    # if running, else queue the download; either way this is idempotent.
    ollama pull "$MODEL" || {
        warn "ollama pull failed. Is the Ollama daemon running? Try: 'ollama serve &' then rerun."
        return 1
    }
}

if [[ "$SKIP_OLLAMA" -eq 1 ]]; then
    info "Skipping Ollama install (--skip-ollama)"
else
    install_ollama || warn "Ollama install skipped; NLU will fall back to the rule-based parser."
    if [[ "$SKIP_MODEL" -eq 1 ]]; then
        info "Skipping model pull (--skip-model)"
    elif have ollama; then
        pull_model || true
    fi
fi

# ---------- 4. Redis ----------
install_redis() {
    if have redis-server; then
        info "Redis already installed: $(redis-server --version | head -n1)"
        return 0
    fi

    local kernel
    kernel="$(uname -s)"
    case "$kernel" in
        Darwin)
            if have brew; then
                info "Installing Redis via Homebrew..."
                brew install redis
            else
                warn "Homebrew not found. Install Redis manually from https://redis.io/download and rerun."
                return 1
            fi
            ;;
        Linux)
            if have apt-get; then
                info "Installing Redis via apt-get (sudo will prompt)..."
                sudo apt-get update -qq
                sudo apt-get install -y redis-server
            elif have dnf; then
                info "Installing Redis via dnf (sudo will prompt)..."
                sudo dnf install -y redis
            elif have yum; then
                info "Installing Redis via yum (sudo will prompt)..."
                sudo yum install -y redis
            else
                warn "No supported package manager found. Install Redis manually."
                return 1
            fi
            ;;
        MINGW*|MSYS*|CYGWIN*)
            warn "Detected Windows shell. Install Redis via WSL or https://redis.io/download."
            return 1
            ;;
        *)
            warn "Unknown platform $kernel. Install Redis manually from https://redis.io/download."
            return 1
            ;;
    esac
}

start_redis() {
    if redis-cli ping &>/dev/null; then
        info "Redis already running (PONG)."
        return 0
    fi

    local kernel
    kernel="$(uname -s)"
    case "$kernel" in
        Darwin)
            if have brew; then
                info "Starting Redis via brew services..."
                brew services start redis &>/dev/null || redis-server --daemonize yes
            else
                redis-server --daemonize yes
            fi
            ;;
        Linux)
            if have systemctl; then
                local unit
                for unit in redis-server redis; do
                    if systemctl list-unit-files 2>/dev/null | grep -q "^${unit}\.service"; then
                        info "Starting Redis via systemctl ($unit)..."
                        sudo systemctl start "$unit" && break
                    fi
                done
            fi
            # Fall back to daemonized mode if systemctl path didn't bring it up.
            if ! redis-cli ping &>/dev/null; then
                info "Starting Redis in daemonized mode..."
                redis-server --daemonize yes
            fi
            ;;
        *)
            redis-server --daemonize yes
            ;;
    esac

    sleep 1
    if redis-cli ping &>/dev/null; then
        info "Redis running (PONG)."
    else
        warn "Redis did not respond to ping. Cache manager will fall back to SQLite."
    fi
}

if [[ "$SKIP_REDIS" -eq 1 ]]; then
    info "Skipping Redis install (--skip-redis)"
else
    install_redis || warn "Redis install skipped; cache manager will use SQLite only."
    if have redis-server; then
        start_redis || true
    fi
fi

# ---------- 5. .env ----------
if [[ ! -f .env ]]; then
    if [[ -f .env.example ]]; then
        info "Copying .env.example to .env (edit this file to add API keys)"
        cp .env.example .env
    else
        warn ".env.example missing; skipping .env creation."
    fi
else
    info ".env already present; not overwriting."
fi

# ---------- 6. DB init ----------
info "Initialising SQLite DB..."
python -c "from src.core import db; db.init_db()"

# ---------- summary ----------
echo
info "Setup complete."
cat <<'EOF'

Next steps:
  1. Edit .env and add your GOOGLE_PLACES_API_KEY (and optionally SERPER_API_KEY).
  2. Activate the venv:  source venv/bin/activate
  3. Run the app:        streamlit run src/app.py
EOF
