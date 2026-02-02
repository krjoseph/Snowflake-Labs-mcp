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

cp docker/server/Dockerfile Dockerfile

# === DEPLOY TO HEROKU ===
echo "🐳 Building and pushing Docker image to Heroku..."
heroku container:push web --app "$HEROKU_APP_NAME"

rm Dockerfile

# === RELEASE CONTAINER ===
echo "🚀 Releasing the container..."
heroku container:release web --app "$HEROKU_APP_NAME"

# === DONE ===
echo "✅ Deployment to Heroku complete."
# Build directory cleanup is handled by trap
