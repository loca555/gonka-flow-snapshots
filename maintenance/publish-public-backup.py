"""Publisher for a dedicated Actions job, never executed on the Akash tenant."""
import argparse
import asyncio
import json
import os
import re
import sqlite3
import sys
import tempfile
from contextlib import closing
from pathlib import Path
import httpx
sys.path.insert(0, "/app")
from app.config import Settings
from app.seed import restore_seed
from app.snapshots import _headers, _releases, publish_snapshot, restore_fresh

REPOSITORY = "loca555/gonka-flow-snapshots"


def checkpoint(database):
    with closing(sqlite3.connect(database)) as db:
        snapshot = json.loads(db.execute("SELECT value FROM kv WHERE key='flow:snapshot'").fetchone()[0])
        return snapshot["height"], snapshot["hash"]


async def verify_public_checkpoint(cfg, height, block_hash=None):
    # GitHub can expose a new release before anonymous API caches see it.
    for delay in (0, 8, 16, 32, 64):
        await asyncio.sleep(delay)
        with tempfile.TemporaryDirectory(prefix="flow-public-restore-") as tmp:
            restored = Path(tmp) / "snapshot.sqlite3"
            if await restore_fresh(restored, cfg):
                observed_height, observed_hash = checkpoint(restored)
                if observed_height == height and (block_hash is None or observed_hash == block_hash):
                    return
    raise ValueError("Published checkpoint could not be restored anonymously after retries")


async def main(directory):
    if os.environ.get("GITHUB_REPOSITORY") != REPOSITORY:
        raise RuntimeError("Publisher must run in the dedicated snapshot repository")
    if os.environ.get("GITHUB_EVENT_NAME") not in ("workflow_dispatch", "schedule") or os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise RuntimeError("Only an approved workflow on main can publish")
    if os.environ.get("FLOW_PUBLISH_APPROVED") != "true":
        raise RuntimeError("Explicit publish input is required")
    token = os.environ.get("BACKUP_GITHUB_TOKEN", "")
    if not token:
        raise RuntimeError("Dedicated job token is not configured")
    folder = Path(directory)
    files = list(folder.iterdir())
    if ({p.name for p in files} != {"manifest.json", "wgnk.sqlite3.gz"}
            or any(p.is_symlink() or not p.is_file() for p in files)):
        raise ValueError("Only the public archive and manifest can be published")
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    with tempfile.TemporaryDirectory(prefix="flow-publisher-") as tmp:
        db = Path(tmp) / "public.sqlite3"
        if not restore_seed(db, folder / "wgnk.sqlite3.gz"):
            raise ValueError("Input archive did not restore")
        source = checkpoint(db)
        if source[0] != manifest["height"]:
            raise ValueError("Input archive height does not match its manifest")
        cfg = Settings(snapshot_repo=REPOSITORY, snapshot_token=token, snapshot_restore=True)
        async with httpx.AsyncClient(timeout=30, headers=_headers()) as client:
            for release in await _releases(client, REPOSITORY):
                tag = re.fullmatch(r"flow-snapshot-(\d+)-[0-9a-f]{12}", release["tag_name"])
                names = {asset["name"] for asset in release.get("assets", []) if asset.get("state") == "uploaded"}
                if tag and int(tag[1]) >= source[0] and names == {"manifest.json", "wgnk.sqlite3.gz"}:
                    existing_height = int(tag[1])
                    await verify_public_checkpoint(cfg, existing_height, source[1] if existing_height == source[0] else None)
                    print(json.dumps({"published": False, "reason": "checkpoint_already_available",
                                      "height": existing_height, "anonymousRestore": True}))
                    return
        height = await publish_snapshot(cfg, db, manifest["height"])
        await verify_public_checkpoint(cfg, *source)
        print(json.dumps({"published": True, "height": height, "anonymousRestore": True, "previousReleasesRetained": True}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory")
    args = parser.parse_args()
    asyncio.run(main(args.directory))
