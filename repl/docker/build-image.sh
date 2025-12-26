#!/bin/bash
# Build Ubuntu-based MySQL 8.0 image with LineairDB support

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_TAG="mysql-lineairdb-ubuntu:8.0.43"

echo "============================================"
echo "Building MySQL 8.0 + LineairDB Image (Ubuntu)"
echo "============================================"

# Build the image
echo "Building Docker image..."
sudo docker build -t "$IMAGE_TAG" -f "$SCRIPT_DIR/Dockerfile" "$SCRIPT_DIR"

echo ""
echo "============================================"
echo "Build complete: $IMAGE_TAG"
echo "============================================"
