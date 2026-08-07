# directory contains: main.go (final server), Containerfile, test_mcp_exec.py
pytest -v test_mcp_exec.py
# optional overrides:
#PODMAN=podman MCP_EXEC_IMAGE=mcp-exec-test pytest -v
