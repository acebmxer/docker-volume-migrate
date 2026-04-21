#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAIN="$SCRIPT_DIR/docker_volume_migrate.py"
REQUIREMENTS="$SCRIPT_DIR/requirements.txt"

# ---------------------------------------------------------------------------
# Colours
# ---------------------------------------------------------------------------
RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'; NC='\033[0m'
info()  { echo -e "${GREEN}[launcher]${NC} $*"; }
warn()  { echo -e "${YELLOW}[launcher]${NC} $*" >&2; }
error() { echo -e "${RED}[launcher] ERROR:${NC} $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1. Python 3
# ---------------------------------------------------------------------------
if ! command -v python3 &>/dev/null; then
    error "python3 not found. Install Python 3.8+ and re-run."
fi
PYTHON=python3

# ---------------------------------------------------------------------------
# 2. Minimum Python version (3.8+)
# ---------------------------------------------------------------------------
if ! "$PYTHON" -c "import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)" 2>/dev/null; then
    error "Python 3.8+ is required. Found: $("$PYTHON" --version 2>&1)"
fi

# ---------------------------------------------------------------------------
# 3. pip
# ---------------------------------------------------------------------------
if ! "$PYTHON" -m pip --version &>/dev/null 2>&1; then
    error "pip is not available for $PYTHON.\n  Fix: sudo apt install python3-pip  (or your distro's equivalent)"
fi

# ---------------------------------------------------------------------------
# 4. Required packages — check imports, install if missing
# ---------------------------------------------------------------------------
if [ ! -f "$REQUIREMENTS" ]; then
    error "requirements.txt not found at $REQUIREMENTS"
fi

MISSING=$("$PYTHON" - <<'EOF' 2>/dev/null
import importlib.util, sys
checks = [
    ("docker",      "docker>=6.0.0"),
    ("rich",        "rich>=13.0.0"),
    ("ruamel.yaml", "ruamel.yaml>=0.17"),
]
missing = [pip_spec for mod, pip_spec in checks if not importlib.util.find_spec(mod)]
print("\n".join(missing))
EOF
)

if [ -n "$MISSING" ]; then
    info "Installing missing packages..."
    PKGS=()
    while IFS= read -r pkg; do PKGS+=("$pkg"); done <<< "$MISSING"

    if "$PYTHON" -m pip install --quiet "${PKGS[@]}" 2>/dev/null; then
        info "Dependencies installed."
    elif "$PYTHON" -m pip install --quiet --break-system-packages "${PKGS[@]}" 2>/dev/null; then
        info "Dependencies installed (--break-system-packages)."
    else
        error "Could not install packages: ${PKGS[*]}\n\n  Try a virtual environment:\n    python3 -m venv .venv && source .venv/bin/activate\n    pip install -r requirements.txt\n  Then run: python3 $MAIN $*"
    fi
else
    info "All dependencies satisfied."
fi

# ---------------------------------------------------------------------------
# 5. Hand off to main script
# ---------------------------------------------------------------------------
exec "$PYTHON" "$MAIN" "$@"
