#!/usr/bin/env python3
"""
Robust checkpointer migration from SQLite to PostgreSQL.
Uses synchronous psycopg2 with small batches.
SQLite: checkpoints -> PostgreSQL: checkpoint_blobs
SQLite: writes -> PostgreSQL: checkpoint_writes
"""
import sqlite3
import psycopg2
from psycopg2.extras import execute_values

# Paths
SQLITE_PATH = "/Users/hyman/Desktop/workspace/github/deer-flow/backend/checkpoints.db"
PG_URL = "postgresql://postgres:postgres!WAN123!!@47.108.135.242:5432/deerflow"

BATCH_SIZE = 20

def get_sqlite_conn():
    return sqlite3.connect(SQLITE_PATH)

def get_pg_conn():
    return psycopg2.connect(PG_URL)

def migrate_checkpoint_blobs():
    """Migrate checkpoints table -> checkpoint_blobs"""
    sqlite_conn = get_sqlite_conn()
    sqlite_conn.row_factory = sqlite3.Row

    pg_conn = get_pg_conn()
    pg_conn.autocommit = True

    cursor = sqlite_conn.execute("SELECT COUNT(*) FROM checkpoints")
    total = cursor.fetchone()[0]
    print(f"Total checkpoint_blobs to migrate: {total}")

    cursor = sqlite_conn.execute("""SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
                                           type, checkpoint, metadata FROM checkpoints""")
    rows = cursor.fetchall()
    sqlite_conn.close()

    migrated = 0

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        values = [(row['thread_id'], row['checkpoint_ns'], row['checkpoint_id'],
                   row['parent_checkpoint_id'], row['type'], row['checkpoint'], row['metadata'])
                  for row in batch]

        try:
            with pg_conn.cursor() as cur:
                execute_values(
                    cur,
                    """INSERT INTO checkpoint_blobs
                       (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, blob, metadata)
                       VALUES %s
                       ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id) DO NOTHING""",
                    values
                )
            migrated += len(batch)
            print(f"  Migrated {migrated}/{total} checkpoint_blobs (batch {i//BATCH_SIZE + 1})")
        except Exception as e:
            print(f"  Error in batch starting at {i}: {e}")
            for row in batch:
                try:
                    with pg_conn.cursor() as cur:
                        cur.execute(
                            """INSERT INTO checkpoint_blobs
                               (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, blob, metadata)
                               VALUES (%s, %s, %s, %s, %s, %s, %s)
                               ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id) DO NOTHING""",
                            (row['thread_id'], row['checkpoint_ns'], row['checkpoint_id'],
                             row['parent_checkpoint_id'], row['type'], row['checkpoint'], row['metadata'])
                        )
                    migrated += 1
                except Exception:
                    pass
            print(f"  After retry: {migrated}/{total}")

    pg_conn.close()
    print(f"\ncheckpoint_blobs migration complete: {migrated} migrated")
    return migrated

def migrate_checkpoint_writes():
    """Migrate writes table -> checkpoint_writes"""
    sqlite_conn = get_sqlite_conn()
    sqlite_conn.row_factory = sqlite3.Row

    pg_conn = get_pg_conn()
    pg_conn.autocommit = True

    cursor = sqlite_conn.execute("SELECT COUNT(*) FROM writes")
    total = cursor.fetchone()[0]
    print(f"Total checkpoint_writes to migrate: {total}")

    cursor = sqlite_conn.execute("""SELECT thread_id, checkpoint_ns, checkpoint_id, task_id, idx,
                                           channel, type, value FROM writes""")
    rows = cursor.fetchall()
    sqlite_conn.close()

    migrated = 0

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        values = [(row['thread_id'], row['checkpoint_ns'], row['checkpoint_id'],
                   row['task_id'], row['idx'], row['channel'], row['type'], row['value'])
                  for row in batch]

        try:
            with pg_conn.cursor() as cur:
                execute_values(
                    cur,
                    """INSERT INTO checkpoint_writes
                       (thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, value)
                       VALUES %s
                       ON CONFLICT (thread_id, checkpoint_id, task_id, idx, channel) DO NOTHING""",
                    values
                )
            migrated += len(batch)
            print(f"  Migrated {migrated}/{total} checkpoint_writes (batch {i//BATCH_SIZE + 1})")
        except Exception as e:
            print(f"  Error in batch starting at {i}: {e}")
            for row in batch:
                try:
                    with pg_conn.cursor() as cur:
                        cur.execute(
                            """INSERT INTO checkpoint_writes
                               (thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, value)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                               ON CONFLICT (thread_id, checkpoint_id, task_id, idx, channel) DO NOTHING""",
                            (row['thread_id'], row['checkpoint_ns'], row['checkpoint_id'],
                             row['task_id'], row['idx'], row['channel'], row['type'], row['value'])
                        )
                    migrated += 1
                except Exception:
                    pass
            print(f"  After retry: {migrated}/{total}")

    pg_conn.close()
    print(f"\ncheckpoint_writes migration complete: {migrated} migrated")
    return migrated

if __name__ == "__main__":
    print("=" * 50)
    print("Starting checkpointer migration")
    print("=" * 50)

    print("\n--- Migrating checkpoint_blobs ---")
    blobs_migrated = migrate_checkpoint_blobs()

    print("\n--- Migrating checkpoint_writes ---")
    writes_migrated = migrate_checkpoint_writes()

    print("\n" + "=" * 50)
    print(f"Migration complete: {blobs_migrated} blobs, {writes_migrated} writes")
    print("=" * 50)