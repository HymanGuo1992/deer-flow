#!/usr/bin/env python3
"""
Migrate channel data from SQLite checkpoints to PostgreSQL checkpoint_blobs table.
LangGraph stores channel values (like messages) in checkpoint_blobs, keyed by channel name + version.
"""
import sqlite3
import msgpack
import psycopg2

SQLITE_PATH = "/Users/hyman/Desktop/workspace/github/deer-flow/backend/checkpoints.db"
PG_URL = "postgresql://postgres:postgres!WAN123!!@47.108.135.242:5432/deerflow"


def dumps_typed(value):
    """Serialize value with type marker (msgpack format)."""
    return msgpack.packb(value, use_bin_type=True)


def migrate_channel_blobs():
    """Extract channel data from checkpoints and store in checkpoint_blobs."""
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sqlite_conn.row_factory = sqlite3.Row

    pg_conn = psycopg2.connect(PG_URL)
    pg_conn.autocommit = True

    cur = sqlite_conn.execute("SELECT COUNT(*) FROM checkpoints")
    total = cur.fetchone()[0]
    print(f"[checkpoint_blobs] Total checkpoints to process: {total}")

    cursor = sqlite_conn.execute("""
        SELECT thread_id, checkpoint_ns, checkpoint_id, checkpoint
        FROM checkpoints
    """)
    rows = cursor.fetchall()
    sqlite_conn.close()

    migrated = 0
    errors = 0

    for i, row in enumerate(rows):
        try:
            cp_data = msgpack.unpackb(row['checkpoint'], raw=False)
            channel_versions = cp_data.get('channel_versions', {})
            channel_values = cp_data.get('channel_values', {})

            for channel, version in channel_versions.items():
                value = channel_values.get(channel)
                if value is None:
                    blob = dumps_typed(None)
                    val_type = "null"
                else:
                    blob = dumps_typed(value)
                    val_type = "msgpack"

                with pg_conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO checkpoint_blobs
                           (thread_id, checkpoint_ns, channel, version, type, blob)
                           VALUES (%s, %s, %s, %s, %s, %s)
                           ON CONFLICT (thread_id, checkpoint_ns, channel, version) DO NOTHING""",
                        (row['thread_id'], row['checkpoint_ns'], channel, version, val_type, blob)
                    )
                if cur.rowcount > 0:
                    migrated += 1

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  Error at row {i}: {e}")
            continue

        if (i + 1) % 1000 == 0:
            print(f"  Progress: {i + 1}/{total}")

    pg_conn.close()
    print(f"[checkpoint_blobs] Migrated: {migrated}, Errors: {errors}")
    return migrated


if __name__ == "__main__":
    print("=" * 60)
    print("Migrating channel blobs to PostgreSQL")
    print("=" * 60)

    b = migrate_channel_blobs()

    print("\n" + "=" * 60)
    print(f"Migration complete: {b} channel blobs")
    print("=" * 60)