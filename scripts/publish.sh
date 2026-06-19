#!/usr/bin/env bash
#
# Publish angarion to PyPI.
#
# Tokens are read from `.secrets` (git-ignored). See `.secrets.example`
# for the expected format.
#
# Usage:
#   scripts/publish.sh           # publish to real PyPI (uses PYPI_TOKEN)
#   scripts/publish.sh --test    # publish to TestPyPI (uses PYPI_TEST_TOKEN)

set -euo pipefail

SECRETS_FILE=".secrets"

if [[ ! -f "$SECRETS_FILE" ]]; then
    echo "ERROR: $SECRETS_FILE not found." >&2
    echo "       Copy .secrets.example to .secrets and fill in your token(s)." >&2
    exit 1
fi

# Source secrets (export every variable defined in the file)
set -a
# shellcheck source=.secrets.example disable=SC1091
. "$SECRETS_FILE"
set +a

# Parse args
USE_TEST=false
if [[ "${1:-}" == "--test" ]]; then
    USE_TEST=true
fi

# Pick token + URL
if "$USE_TEST"; then
    if [[ -z "${PYPI_TEST_TOKEN:-}" || "$PYPI_TEST_TOKEN" == "pypi-REPLACE-ME" ]]; then
        echo "ERROR: PYPI_TEST_TOKEN is missing or not filled in $SECRETS_FILE" >&2
        exit 1
    fi
    TOKEN="$PYPI_TEST_TOKEN"
    PUBLISH_URL="https://test.pypi.org/legacy/"
    echo "→ Target: TestPyPI"
else
    if [[ -z "${PYPI_TOKEN:-}" || "$PYPI_TOKEN" == "pypi-REPLACE-ME" ]]; then
        echo "ERROR: PYPI_TOKEN is missing or not filled in $SECRETS_FILE" >&2
        exit 1
    fi
    TOKEN="$PYPI_TOKEN"
    PUBLISH_URL=""
    echo "→ Target: PyPI (production)"
fi

# 1. Build
echo "→ Building wheel + sdist..."
rm -rf dist/
uv build

# 2. Validate
echo "→ Validating artefacts with twine check..."
uv run twine check dist/*

# 3. Publish
echo "→ Uploading..."
if "$USE_TEST"; then
    UV_PUBLISH_TOKEN="$TOKEN" uv publish --publish-url "$PUBLISH_URL"
else
    UV_PUBLISH_TOKEN="$TOKEN" uv publish
fi

echo ""
echo "✓ Published successfully."
if "$USE_TEST"; then
    echo ""
    echo "Verify (TestPyPI):"
    echo "  uvx --index https://test.pypi.org/simple/ \\"
    echo "      --extra-index https://pypi.org/simple/ \\"
    echo "      --with angarion python -c 'import angarion; print(angarion.__version__)'"
else
    echo ""
    echo "Verify (after ~1-2 min for indexing):"
    echo "  uvx --with angarion python -c 'import angarion; print(angarion.__version__)'"
fi
