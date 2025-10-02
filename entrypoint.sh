#!/bin/sh

GRAPH_FILE="irpin_drive_graph.graphml"

if [ ! -f "$GRAPH_FILE" ]; then
    echo "Graph file '$GRAPH_FILE' not found. Running the script to create it..."
    python prepare_graph.py
else
    echo "Graph file '$GRAPH_FILE' already exists. Skipping creation step."
fi

exec "$@"