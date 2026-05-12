#!/usr/bin/env python3
"""
Migrate checkpoints from SQLite (msgpack) to PostgreSQL (BYTEA format).
LangGraph PostgreSQL checkpointer uses BYTEA for blob columns.
"""
import sqlite3
import msgpack
import json
import base64
import psycopg2

SQLITE_PATH = "/Users/hyman/Desktop/workspace/github/deer-flow/backend/checkpoints.db"
PG_URL = "postgresql://postgres:postgres!WAN123!!@47.108.135.242:5432/deerflow"


def convert_msgpack(obj):
    """Recursively convert msgpack data including ExtType to JSON-serializable."""
    if isinstance(obj, msgpack.ext.ExtType):
        if obj.data:
            return {"__msgpack_ext__": "bin", "data": base64.b64encode(obj.data).decode('ascii'), "code": obj.code}
        return None
    elif isinstance(obj, dict):
        return {k: convert_msgpack(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_msgpack(item) for item in obj]
    elif isinstance(obj, bytes):
        return base64.b64encode(obj).decode('ascii')
    return obj


def migrate_checkpoints():
    """Migrate checkpoints table."""
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sqlite_conn.row_factory = sqlite3.Row

    pg_conn = psycopg2.connect(PG_URL)
    pg_conn.autocommit = True

    cur = sqlite_conn.execute("SELECT COUNT(*) FROM checkpoints")
    total = cur.fetchone()[0]
    print(f"[checkpoints] Total: {total}")

    cursor = sqlite_conn.execute("""
        SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata
        FROM checkpoints
    """)
    rows = cursor.fetchall()
    sqlite_conn.close()

    migrated = 0
    errors = 0

    for i, row in enumerate(rows):
        try:
            cp_data = msgpack.unpackb(row['checkpoint'], raw=False)
            cp = convert_msgpack(cp_data)
            md = json.loads(row['metadata'].decode('utf-8')) if row['metadata'] else {}

            with pg_conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO checkpoints
                       (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id) DO NOTHING""",
                    (row['thread_id'], row['checkpoint_ns'], row['checkpoint_id'],
                     row['parent_checkpoint_id'], row['type'], json.dumps(cp), json.dumps(md))
                )
            if cur.rowcount > 0:
                migrated += 1

        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  Error row {i}: {e}")
            continue

        if (i + 1) % 1000 == 0:
            print(f"  Progress: {i + 1}/{total}")

    pg_conn.close()
    print(f"[checkpoints] Migrated: {migrated}, Errors: {errors}")
    return migrated


def migrate_writes():
    """Migrate writes table to checkpoint_writes (BYTEA format)."""
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sqlite_conn.row_factory = sqlite3.Row

    pg_conn = psycopg2.connect(PG_URL)
    pg_conn.autocommit = True

    cur = sqlite_conn.execute("SELECT COUNT(*) FROM writes")
    total = cur.fetchone()[0]
    print(f"\n[checkpoint_writes] Total: {total}")

    cursor = sqlite_conn.execute("""
        SELECT thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, value
        FROM writes
    """)
    rows = cursor.fetchall()
    sqlite_conn.close()

    migrated = 0
    errors = 0

    for i, row in enumerate(rows):
        try:
            # value is msgpack BLOB - store raw bytes in BYTEA column
            blob_bytes = row['value'] if row['value'] else b''

            with pg_conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO checkpoint_writes
                       (thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, blob)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id, task_id, idx) DO NOTHING""",
                    (row['thread_id'], row['checkpoint_ns'], row['checkpoint_id'],
                     row['task_id'], row['idx'], row['channel'], row['type'], blob_bytes)
                )
            if cur.rowcount > 0:
                migrated += 1

        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  Error row {i}: {e}")
            continue

        if (i + 1) % 1000 == 0:
            print(f"  Progress: {i + 1}/{total}")

    pg_conn.close()
    print(f"[checkpoint_writes] Migrated: {migrated}, Errors: {errors}")
    return migrated


if __name__ == "__main__":
    print("=" * 60)
    print("Migrating SQLite checkpoints to PostgreSQL (BYTEA)")
    print("=" * 60)

    b = migrate_checkpoints()
    w = migrate_writes()

    print("\n" + "=" * 60)
    print(f"Migration complete: {b} checkpoints, {w} writes")
    print("=" * 60)