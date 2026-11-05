"""Rebuild a public recovery seed without mounting any operator/tenant database.

Run inside the pinned public Flow image. Output contains only manifest + archive.
No publisher credential is accepted by this process.
"""
import argparse
import asyncio
import json
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path
from contextlib import nullcontext
import httpx

sys.path.insert(0, "/app")
from app.eth_rpc import EthereumRPCError, retry_after
from app.config import Settings
from app.db import Database
from app.mints import MintIndexer
from app.seed import export_seed, restore_seed
from app.snapshots import restore_fresh


async def retry_read(operation, *args, **kwargs):
    for attempt in range(5):
        try:
            return await operation(*args, **kwargs)
        except (EthereumRPCError, httpx.HTTPError) as error:
            if isinstance(error, EthereumRPCError):
                if error.category not in ('unavailable', 'rate_limit', 'cooldown'): raise
                wait = error.retry_after
            elif isinstance(error, httpx.HTTPStatusError):
                if error.response.status_code != 429 and error.response.status_code < 500: raise
                wait = retry_after(error.response.headers.get('Retry-After'))
            else:
                wait = 0
            if attempt == 4: raise
            delay = max(8 * 2**attempt, wait)
            print(json.dumps({'readRetry': attempt+1, 'delaySeconds': delay,
                              'category': getattr(error, 'category', type(error).__name__)}), file=sys.stderr, flush=True)
            await asyncio.sleep(delay)


async def main(args):
    workspace = nullcontext(args.work) if args.work else tempfile.TemporaryDirectory(prefix="flow-backup-")
    with workspace as temporary:
        work = Path(temporary)
        work.mkdir(parents=True, exist_ok=True)
        cfg = Settings(mode="mints", data_dir=work, indexer_enabled=False,
                       snapshot_token="", snapshot_restore=not args.offline,
                       snapshot_repo="loca555/gonka-flow-snapshots", powder_enabled=False)
        # Both are existing public providers; Sources retains per-host rate
        # limiting, access checks and Retry-After across the live/archive lanes.
        cfg.ethereum_archive = list(dict.fromkeys(cfg.ethereum_archive + cfg.ethereum))
        database = work / "public.sqlite3"
        await restore_fresh(database, cfg)
        db = Database(database)
        indexer = MintIndexer(cfg, db)
        original_eth = indexer.net.eth
        async def resilient_eth(*a, **kw):
            return await retry_read(original_eth, *a, **kw)
        indexer.net.eth = resilient_eth
        original = db.get("flow:snapshot")["height"]
        target, canonical_hash = original, db.get("flow:snapshot")["hash"]
        try:
            if not args.offline:
                # The same pinned header must agree across both independent public RPCs.
                async def raw_rpc(endpoint, method, params):
                    response = await indexer.net.client.post(endpoint, json={
                        "jsonrpc": "2.0", "id": 1, "method": method, "params": params})
                    response.raise_for_status()
                    body = response.json()
                    if body.get("error") or "result" not in body:
                        raise ValueError("Public RPC did not return a verifiable header")
                    return body["result"]
                async def rpc(*a):
                    return await retry_read(raw_rpc, *a)
                nodes = ["https://rpc.mevblocker.io", "https://ethereum-rpc.publicnode.com"]
                heads = await asyncio.gather(*(rpc(node, "eth_getBlockByNumber", ["finalized", False]) for node in nodes))
                latest_final = min(int(head["number"], 16) for head in heads)
                if latest_final < original:
                    raise ValueError("Finalized height moved backwards")
                if not args.max_advance and latest_final - original > 30000:
                    raise ValueError("Public seed is too old for the bounded backup job")
                target = min(latest_final, original + args.max_advance) if args.max_advance else latest_final
                headers = await asyncio.gather(*(rpc(node, "eth_getBlockByNumber", [hex(target), False]) for node in nodes))
                if (any(not header or int(header["number"], 16) != target for header in headers)
                        or headers[0]["hash"].lower() != headers[1]["hash"].lower()):
                    raise ValueError("Independent RPC checkpoint mismatch")
                canonical_hash = headers[0]["hash"].lower()
                await indexer.setup(target)
                previous = db.get("flow:snapshot")
                indexer.flows.pools = {pool["address"]: pool for pool in previous["pools"]}
                for begin in range(original + 1, target + 1, 1000):
                    end = min(target, begin + 999)
                    def covered(scope):
                        return db.conn.execute('SELECT 1 FROM ranges WHERE chain=? AND lo<=? AND hi>=?', (scope, begin, end)).fetchone() is not None
                    if not covered('wgnk_mints'):
                        await indexer.batch(begin, end, archive=True, next_height=end + 1)
                    if not covered('ethereum'):
                        await indexer.flows.batch(begin, end, archive=True)
                    print(json.dumps({'verifiedThrough':end,'target':target}),file=sys.stderr,flush=True)
                indexer.status("mints:status", finalized_height=target, latest_height=target,
                               latest_ts=int(headers[0]["timestamp"], 16), indexed_height=target)
                await indexer.flows.snapshot(db.get("mints:deployment")["height"], target)
                if db.get("flow:snapshot")["hash"].lower() != canonical_hash:
                    raise ValueError("Export snapshot differs from pinned checkpoint")
            with tempfile.TemporaryDirectory(prefix="flow-clean-export-") as checked:
                staging = Path(checked) / "export"
                manifest = export_seed(database, staging)
                if manifest["height"] != target:
                    raise ValueError("Export did not reach requested target")
                restored = Path(checked) / "restored.sqlite3"
                if not restore_seed(restored, staging / "wgnk.sqlite3.gz"):
                    raise ValueError("Restoration failed")
                with sqlite3.connect(restored) as connection:
                    if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                        raise ValueError("Restored SQLite failed integrity check")
                    restored_snapshot = json.loads(connection.execute(
                        "SELECT value FROM kv WHERE key='flow:snapshot'").fetchone()[0])
                    if (restored_snapshot["height"], restored_snapshot["hash"].lower()) != (target, canonical_hash):
                        raise ValueError("Restored snapshot does not match the verified checkpoint")
                output = Path(args.output)
                output.mkdir(parents=True, exist_ok=True)
                if any(output.iterdir()):
                    raise ValueError("Output directory must be empty; previous backups are never overwritten")
                for name in ("manifest.json", "wgnk.sqlite3.gz"):
                    shutil.copyfile(staging / name, output / name)
                print(json.dumps({"status": "passed", "offline": args.offline,
                                  "boundedSmoke": bool(args.max_advance), "sourceHeight": original,
                                  "height": target, "blockHash": canonical_hash, "manifest": manifest,
                                  "integrity": "ok", "restoreVerified": True, "productionDatabaseMounted": False,
                                  "publicTransactions": 0, "published": False}))
        finally:
            await indexer.stop()
            db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--work", help="Optional dedicated public checkpoint directory for retrying a local run")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--max-advance", type=int, default=0)
    arguments = parser.parse_args()
    if not 0 <= arguments.max_advance <= 30000:
        parser.error("max-advance must be between 0 and 30000")
    asyncio.run(asyncio.wait_for(main(arguments), timeout=1800))

