#!/usr/bin/env python3
"""Migrate checkpoints from SQLite to PostgreSQL with correct LangGraph schema."""
import sqlite3
import asyncio
import asyncpg

SQLITE_PATH = "/Users/hyman/Desktop/workspace/github/deer-flow/backend/checkpoints.db"
PG_URL = "postgresql://postgres:postgres!WAN123!!@47.108.135.242:5432/deerflow"

BATCH_SIZE = 20

def get_sqlite_conn():
    return sqlite3.connect(SQLITE_PATH)

async def migrate():
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sqlite_conn.row_factory = sqlite3.Row

    # Migrate checkpoints -> checkpoints
    print("Migrating checkpoints...")
    cursor = sqlite_conn.execute("SELECT COUNT(*) FROM checkpoints")
    total = cursor.fetchone()[0]
    print(f"Total: {total}")

    cursor = sqlite_conn.execute("""SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
                                           type, checkpoint, metadata FROM checkpoints""")
    rows = cursor.fetchall()

    # Connect to PostgreSQL
    conn = await asyncpg.connect(PG_URL)

    migrated = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        values = [(row['thread_id'], row['checkpoint_ns'], row['checkpoint_id'],
                   row['parent_checkpoint_id'], row['type'], bytes(row['checkpoint']) if row['checkpoint'] else None,
                   bytes(row['metadata']) if row['metadata'] else None)
                  for row in batch]

        for vals in values:
            await conn.execute(
                """INSERT INTO checkpoints (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id) DO NOTHING""",
                *vals
            )
        migrated += len(batch)
        print(f"  Migrated {migrated}/{total}")

    print(f"Checkpoints migrated: {migrated}")

    # Migrate writes -> checkpoint_writes
    print("\nMigrating checkpoint_writes...")
    cursor = sqlite_conn.execute("SELECT COUNT(*) FROM writes")
    total = cursor.fetchone()[0]
    print(f"Total: {total}")

    cursor = sqlite_conn.execute("""SELECT thread_id, checkpoint_ns, checkpoint_id, task_id, idx,
                                           channel, type, value FROM writes""")
    rows = cursor.fetchall()

    migrated = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        values = [(row['thread_id'], row['checkpoint_ns'], row['checkpoint_id'],
                   row['task_id'], row['idx'], row['channel'], row['type'],
                   bytes(row['value']) if row['value'] else None)
                  for row in batch]

        for vals in values:
            await conn.execute(
                """INSERT INTO checkpoint_writes (thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, value)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                   ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id, task_id, idx) DO NOTHING""",
                *vals
            )
        migrated += len(batch)
        print(f"  Migrated {migrated}/{total}")

    print(f"\ncheckpoint_writes migrated: {migrated}")
    await conn.close()
    sqlite_conn.close()

if __name__ == "__main__":
    asyncio.run(migrate())