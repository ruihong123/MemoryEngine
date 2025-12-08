#!/bin/bash
CORE_FILE="/mnt/core_dump/corenode-6.19563"

if [ -f "$CORE_FILE" ]; then
    echo "Core file exists: $CORE_FILE"
    echo "Size: $(ls -lh $CORE_FILE | awk '{print $5}')"
    file "$CORE_FILE"
else
    echo "Core file NOT found: $CORE_FILE"
    echo "Checking for core files..."
    find /mnt -name "core*" -o -name "corenode*" 2>/dev/null | head -10
fi
