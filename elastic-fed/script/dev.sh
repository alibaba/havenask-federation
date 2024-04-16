#!/bin/bash

BASHRC_FILE="$HOME/.bashrc"

TO_ADD=$(cat <<'EOF'
export JAVA11_HOME="/usr/share/havenask/jdk"
export JAVA_HOME="$JAVA11_HOME"
export PATH=$JAVA_HOME/bin:$PATH
EOF
)

if ! grep -q "JAVA11_HOME=\"/usr/share/havenask/jdk\"" "$BASHRC_FILE"; then
    echo "$TO_ADD" >> "$BASHRC_FILE"
fi

source "$BASHRC_FILE"