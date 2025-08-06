#!/bin/bash

# Create build directory
mkdir -p build

# Build the presentation with navigation controls
npx @marp-team/marp-cli@latest docs/idempotency-presentation.md \
  -o build/index.html \
  --html \
  --theme default


echo "Build complete! Files in build/"
