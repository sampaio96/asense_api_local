#!/bin/bash

# Script Name: deploy.sh
# Description: Packages the asense_api_local project for AWS Lambda deployment.

# 1. Configuration
PROJECT_DIR="."
ZIP_NAME="deploy.zip"
SCRIPT_NAME=$(basename "$0") # Gets the name of this script (e.g., deploy.sh)

# 2. Cleanup Old Package
if [ -f "$ZIP_NAME" ]; then
    echo "🗑️  Removing old $ZIP_NAME..."
    rm "$ZIP_NAME"
fi

# 3. Create Zip Package
echo "📦 Packaging Lambda function..."

# Use 'zip' command to recursively add files
# -r : Recursive
# -9 : Max compression
# -x : Exclude patterns
zip -r -9 "$ZIP_NAME" "$PROJECT_DIR" \
    -x "*.git*" \
    -x "*.idea*" \
    -x "*.DS_Store*" \
    -x "*.venv*" \
    -x "*__pycache__*" \
    -x "*tests/*" \
    -x "run_local.py" \
    -x "$SCRIPT_NAME" \
    -x "$ZIP_NAME" \
    -x "*/.*" 

# 4. Verify Result
if [ -f "$ZIP_NAME" ]; then
    FILE_SIZE=$(du -h "$ZIP_NAME" | cut -f1)
    echo ""
    echo "✅ Success! Package created: $ZIP_NAME ($FILE_SIZE)"
    echo ""
    echo "🚀 DEPLOYMENT INSTRUCTIONS:"
    echo "-------------------------------------------------------"
    echo "1. Go to the AWS Lambda Console."
    echo "2. Select your function."
    echo "3. Click 'Upload from' -> '.zip file'."
    echo "4. Select '$ZIP_NAME' from this directory."
    echo "5. Ensure Runtime Settings > Handler is set to: 'lambda_function.lambda_handler'"
    echo "-------------------------------------------------------"
else
    echo "❌ Error: Failed to create zip file."
    exit 1
fi