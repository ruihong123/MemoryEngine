#!/bin/bash

# Script to regenerate compile_commands.json for IntelliSense

echo "Regenerating compile_commands.json for IntelliSense..."

# Clean and recreate build directory
rm -rf build
mkdir -p build

# Configure CMake with compile commands export
cd build
cmake .. -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

# Copy compile_commands.json to project root
cp compile_commands.json ../

echo "compile_commands.json has been regenerated and copied to project root."
echo "IntelliSense should now work properly in VS Code."
