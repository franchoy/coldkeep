#!/usr/bin/env python3
"""Fail-closed validation for v1.13.15 development and product containers."""

from __future__ import annotations

import json
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parent.parent
GO_INDEX = "sha256:e8c859f5632dcfde7b32d2012b4351728f6437930887c2f6a91ea242459e5514"
POSTGRES_INDEX = "sha256:bb3e1a57e5407e0a5280b4211980a5e537f4abd234a87014ac979849a78dd825"
ALPINE_INDEX = "sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce"


def fail(message: str) -> None:
    print(f"PHASE3_CONTRACT_ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def read(relative: str) -> str:
    path = ROOT / relative
    if not path.is_file() or path.is_symlink():
        fail(f"{relative} must be a regular non-symlink file")
    return path.read_text(encoding="utf-8")


dockerfile = read("Dockerfile")
dev_dockerfile = read(".devcontainer/Dockerfile")
product_compose = read("docker-compose.yml")
dev_compose = read(".devcontainer/docker-compose.yml")
post_create = read(".devcontainer/post-create.sh")
env_example = read(".env.example")
dockerignore = read(".dockerignore")
devcontainer = json.loads(read(".devcontainer/devcontainer.json"))

required_images = {
    "product builder": (dockerfile, rf"golang:1\.26\.7-bookworm@{GO_INDEX}"),
    "development base": (dev_dockerfile, rf"golang:1\.26\.7-bookworm@{GO_INDEX}"),
    "product runtime": (dockerfile, rf"alpine:3\.22\.5@{ALPINE_INDEX}"),
    "product postgres": (product_compose, rf"postgres:16\.15-bookworm@{POSTGRES_INDEX}"),
    "development postgres": (dev_compose, rf"postgres:16\.15-bookworm@{POSTGRES_INDEX}"),
}
for label, (source, pattern) in required_images.items():
    if not re.search(pattern, source):
        fail(f"{label} is not pinned to the verified official index")

if "FROM --platform=$BUILDPLATFORM golang:" not in dockerfile:
    fail("product builder must execute on BUILDPLATFORM for deterministic cross-builds")

combined_dev = "\n".join((dev_dockerfile, dev_compose, json.dumps(devcontainer)))
for prohibited in ("privileged:", "/var/run/docker.sock", "docker.sock", "host.docker.internal"):
    if prohibited in combined_dev:
        fail(f"development container contains prohibited default: {prohibited}")

if "COLDKEEP_KEY" in dev_compose:
    fail("development compose must not inject encryption key material")
if re.search(r"^COLDKEEP_KEY=", env_example, re.MULTILINE):
    fail(".env.example must not contain an active/shared key assignment")
for required_ignore in (".git", ".env", "storage"):
    if required_ignore not in dockerignore.splitlines():
        fail(f"Docker build context must exclude {required_ignore}")
if devcontainer.get("postCreateCommand") != "bash .devcontainer/post-create.sh":
    fail("devcontainer post-create command drift")
if "GOTOOLCHAIN=local" not in dockerfile or "go1.26.7" not in post_create:
    fail("exact local Go toolchain enforcement is missing")
if "git config --global --get-all safe.directory" not in post_create:
    fail("post-create bootstrap is not guarded for idempotency")

print("PHASE3_CONTAINER_CONTRACTS: PASS")
