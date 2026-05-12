#!/usr/bin/env python3
"""Fast checkpoint migration using COPY command."""
import sqlite3
import asyncio
import asyncpg
import io

SQLITE_PATH = "/Users/hyman/Desktop/workspace/github/deer-flow/backend/checkpoints.db"
PG_URL = "postgresql://postgres:postgres!WAN123!!@47.108.135.242:5432/deerflow"

async def migrate():
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sqlite_conn.row_factory = sqlite3.Row

    conn = await asyncpg.connect(PG_URL)

    # Migrate checkpoints
    print("Migrating checkpoints...")
    cursor = sqlite_conn.execute("SELECT COUNT(*) FROM checkpoints")
    total = cursor.fetchone()[0]
    print(f"Total: {total}")

    cursor = sqlite_conn.execute("""SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
                                           type, checkpoint, metadata FROM checkpoints""")
    rows = cursor.fetchall()

    buf = io.BytesIO()
    for row in rows:
        vals = (
            row['thread_id'],
            row['checkpoint_ns'],
            row['checkpoint_id'],
            row['parent_checkpoint_id'] or '',
            row['type'] or '',
            bytes(row['checkpoint']) if row['checkpoint'] else b'',
            bytes(row['metadata']) if row['metadata'] else b''
        )
        line = '\t'.join('' if v is None else v.decode('latin1') if isinstance(v, bytes) else str(v) for v in vals) + '\n'
        buf.write(line.encode('utf8'))
    buf.seek(0)

    await conn.copy_to_table('checkpoints', source=buf, format='csv', delimiter='\t', null='')
    print(f"Checkpoints migrated: {total}")

    # Migrate checkpoint_writes
    print("\nMigrating checkpoint_writes...")
    cursor = sqlite_conn.execute("SELECT COUNT(*) FROM writes")
    total = cursor.fetchone()[0]
    print(f"Total: {total}")

    cursor = sqlite_conn.execute("""SELECT thread_id, checkpoint_ns, checkpoint_id, task_id, idx,
                                           channel, type, value FROM writes""")
    rows = cursor.fetchall()

    buf = io.BytesIO()
    for row in rows:
        vals = (
            row['thread_id'],
            row['checkpoint_ns'],
            row['checkpoint_id'],
            row['task_id'],
            str(row['idx']),
            row['channel'],
            row['type'] or '',
            bytes(row['value']) if row['value'] else b''
        )
        line = '\t'.join('' if v is None else v.decode('latin1') if isinstance(v, bytes) else str(v) for v in vals) + '\n'
        buf.write(line.encode('utf8'))
    buf.seek(0)

    await conn.copy_to_table('checkpoint_writes', source=buf, format='csv', delimiter='\t', null='')
    print(f"checkpoint_writes migrated: {total}")

    await conn.close()
    sqlite_conn.close()
    print("\nMigration complete!")

if __name__ == "__main__":
    asyncio.run(migrate())
