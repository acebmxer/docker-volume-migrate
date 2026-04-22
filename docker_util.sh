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
# Helper: install a system package via the available package manager
# ---------------------------------------------------------------------------
pkg_install() {
    local apt_pkg="$1" dnf_pkg="$2" yum_pkg="$3" pac_pkg="$4" label="$5"
    if command -v apt-get &>/dev/null; then
        sudo apt-get install -y "$apt_pkg" || error "Failed to install $label via apt-get."
    elif command -v dnf &>/dev/null; then
        sudo dnf install -y "$dnf_pkg" || error "Failed to install $label via dnf."
    elif command -v yum &>/dev/null; then
        sudo yum install -y "$yum_pkg" || error "Failed to install $label via yum."
    elif command -v pacman &>/dev/null; then
        sudo pacman -Sy --noconfirm "$pac_pkg" || error "Failed to install $label via pacman."
    else
        error "$label is not available and no supported package manager found. Install it manually and re-run."
    fi
}

# ---------------------------------------------------------------------------
# 1. Python 3
# ---------------------------------------------------------------------------
if ! command -v python3 &>/dev/null; then
    warn "python3 not found — attempting to install..."
    pkg_install python3 python3 python3 python "Python 3"
    command -v python3 &>/dev/null || error "python3 still not found after installation attempt."
    info "Python 3 installed successfully."
fi
PYTHON=python3

# ---------------------------------------------------------------------------
# 2. Minimum Python version (3.8+)
# ---------------------------------------------------------------------------
if ! "$PYTHON" -c "import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)" 2>/dev/null; then
    error "Python 3.8+ is required. Found: $("$PYTHON" --version 2>&1). Upgrade Python and re-run."
fi

# ---------------------------------------------------------------------------
# 3. pip
# ---------------------------------------------------------------------------
if ! "$PYTHON" -m pip --version &>/dev/null 2>&1; then
    warn "pip not found for $PYTHON — attempting to install python3-pip..."
    pkg_install python3-pip python3-pip python3-pip python-pip "pip"
    if ! "$PYTHON" -m pip --version &>/dev/null 2>&1; then
        error "pip still not available after installation attempt."
    fi
    info "pip installed successfully."
fi

# ---------------------------------------------------------------------------
# 4. Required packages — check imports, install if missing
# ---------------------------------------------------------------------------
if [ ! -f "$REQUIREMENTS" ]; then
    error "requirements.txt not found at $REQUIREMENTS"
fi

MISSING=$("$PYTHON" - <<'EOF' 2>/dev/null || true
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
    while IFS= read -r pkg; do PKGS+=("$pkg"); done <<< "$MISSING" || true

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
