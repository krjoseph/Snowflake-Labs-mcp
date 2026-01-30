#!/bin/bash

set -e

# === CONFIGURATION ===
HEROKU_APP_NAME="snowflake-mcp"

# Get the script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# === LOGIN TO HEROKU CONTAINER REGISTRY ===
echo "🔐 Logging in to Heroku Container Registry..."
heroku container:login

# === SET HEROKU STACK TO CONTAINER ===
echo "🔧 Setting Heroku stack to 'container' ..."
heroku stack:set container --app "$HEROKU_APP_NAME"

# === PREPARE TEMPORARY BUILD DIRECTORY ===
echo "📦 Preparing build directory from local repository..."
BUILD_DIR="$(mktemp -d)"
trap "rm -rf $BUILD_DIR" EXIT

# Copy the entire project to build directory (excluding git and build artifacts)
echo "📋 Copying project files..."
rsync -av \
    --exclude='.git' \
    --exclude='temp-repo' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='.venv' \
    --exclude='.ruff_cache' \
    --exclude='.mypy_cache' \
    "$PROJECT_ROOT/" "$BUILD_DIR/"

cd "$BUILD_DIR"

# === CREATE DOCKERFILE IN ROOT ===
echo "📄 Creating Dockerfile in root for Heroku build..."
# Use the Heroku-specific Dockerfile that uses uvx (same as local usage)
# This avoids circular import issues by running via uvx instead of installing as package
# Heroku expects Dockerfile in root, so we copy it there
if [ ! -f "docker/server/Dockerfile.heroku" ]; then
    echo "❌ Error: docker/server/Dockerfile.heroku not found!"
    exit 1
fi
cp docker/server/Dockerfile.heroku Dockerfile
echo "✅ Using Dockerfile.heroku for deployment"

# === DEPLOY TO HEROKU ===
echo "🐳 Building and pushing Docker image to Heroku..."
heroku container:push web --app "$HEROKU_APP_NAME"

# === CLEANUP TEMPORARY DOCKERFILE ===
rm -f Dockerfile

# === RELEASE CONTAINER ===
echo "🚀 Releasing the container..."
heroku container:release web --app "$HEROKU_APP_NAME"

# === DONE ===
echo "✅ Deployment to Heroku complete."
# Build directory cleanup is handled by trap
