#!/bin/sh
podman image untag localhost/mcp_execute:latest
podman image untag ghcr.io/andy7d0/mcp_execute:latest
podman build -f Containerfile-big --tag mcp_execute:latest --network=host .
podman tag localhost/mcp_execute:latest ghcr.io/andy7d0/mcp_execute:latest
podman push ghcr.io/andy7d0/mcp_execute:latest