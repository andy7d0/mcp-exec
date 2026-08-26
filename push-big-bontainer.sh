#!/bin/sh
podman build -f Containerfile-big --tag mcp_execute .
podman tag mcp_execute:latest ghcr.io/andy7d0/mcp_execute:latest
podman push ghcr.io/andy7d0/mcp_execute:latest