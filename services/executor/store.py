import secrets
from collections.abc import Sequence
from typing import Any

from psycopg_pool import AsyncConnectionPool

from .models import (
    RUN_PENDING,
    RUN_RUNNING,
    STEP_DISPATCHED,
    Device,
    Run,
    Step,
)
from .workflows import StepTemplate


class RunNotFound(Exception):
    """No run with that id."""


def new_id(prefix: str) -> str:
    return f"{prefix}-{secrets.token_hex(6)}"


RUN_COLS = "id, workflow_name, status, created_at, started_at, finished_at"
STEP_COLS = (
    "id, run_id, name, device_id, status, depends_on, "
    "dispatch_count, dispatched_at, finished_at, error"
)


def _run(row: Sequence[Any]) -> Run:
    return Run(*row)


def _step(row: Sequence[Any]) -> Step:
    return Step(*row)


class Store:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool

    async def list_devices(self) -> list[Device]:
        async with self._pool.connection() as conn:
            cur = await conn.execute("SELECT id, name, type FROM devices ORDER BY id")
            return [Device(*row) for row in await cur.fetchall()]

    async def list_runs(self) -> list[Run]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(f"SELECT {RUN_COLS} FROM runs ORDER BY created_at DESC")
            return [_run(row) for row in await cur.fetchall()]

    async def get_run(self, run_id: str) -> Run:
        async with self._pool.connection() as conn:
            cur = await conn.execute(f"SELECT {RUN_COLS} FROM runs WHERE id = %s", (run_id,))
            row = await cur.fetchone()
        if row is None:
            raise RunNotFound(run_id)
        return _run(row)

    async def create_run(self, workflow_name: str, template: list[StepTemplate]) -> Run:
        """Materialise a workflow template into a run and its steps."""
        run_id = new_id("run")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                f"INSERT INTO runs (id, workflow_name) VALUES (%s, %s) RETURNING {RUN_COLS}",
                (run_id, workflow_name),
            )
            row = await cur.fetchone()
            for st in template:
                await conn.execute(
                    "INSERT INTO steps (id, run_id, name, device_id, depends_on)"
                    " VALUES (%s, %s, %s, %s, %s)",
                    (new_id("step"), run_id, st.name, st.device_id, list(st.depends_on)),
                )
        assert row is not None
        return _run(row)

    async def list_steps(self, run_id: str) -> list[Step]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                f"SELECT {STEP_COLS} FROM steps WHERE run_id = %s ORDER BY name",
                (run_id,),
            )
            return [_step(row) for row in await cur.fetchall()]

    async def start_run(self, run_id: str) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE runs SET status = %s, started_at = now(), updated_at = now()"
                " WHERE id = %s AND status = %s",
                (RUN_RUNNING, run_id, RUN_PENDING),
            )

    async def finish_run(self, run_id: str, status: str) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE runs SET status = %s, finished_at = now(), updated_at = now()"
                " WHERE id = %s",
                (status, run_id),
            )

    async def record_step_dispatched(self, step_id: str) -> None:
        """Note that a step has been sent to its driver."""
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE steps"
                "   SET status = %s,"
                "       dispatch_count = dispatch_count + 1,"
                "       dispatched_at = COALESCE(dispatched_at, now()),"
                "       updated_at = now()"
                " WHERE id = %s",
                (STEP_DISPATCHED, step_id),
            )

    async def record_step_finished(self, step_id: str, status: str, error: str = "") -> None:
        """Note that a step finished, successfully or not."""
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE steps SET status = %s, error = %s,"
                " finished_at = now(), updated_at = now()"
                " WHERE id = %s",
                (status, error or None, step_id),
            )

    # --- Scheduler Helper Methods ---

    async def get_steps(self, run_id: str) -> list[Step]:
        """Alias for list_steps to match scheduler interface."""
        return await self.list_steps(run_id)

    async def is_device_busy(self, device_id: str) -> bool:
        """Check if any step on this device is currently dispatched/running."""
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT 1 FROM steps WHERE device_id = %s AND status = %s LIMIT 1",
                (device_id, STEP_DISPATCHED)
            )
            return await cur.fetchone() is not None

    async def is_failed(self, run_id: str) -> bool:
        """Check if the run has failed."""
        run = await self.get_run(run_id)
        return run.status == "failed"

    async def set_workflow_failed(self, run_id: str, step_id: str, error: str) -> None:
        """Mark a step as failed and fail the entire run."""
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE steps SET status = 'failed', error = %s, finished_at = now(), updated_at = now() WHERE id = %s",
                (error, step_id)
            )
            await conn.execute(
                "UPDATE runs SET status = 'failed', finished_at = now(), updated_at = now() WHERE id = %s",
                (run_id,)
            )

    async def mark_completed(self, run_id: str, step_name: str) -> None:
        """Mark a specific step name as completed for a run."""
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE steps SET status = 'completed', finished_at = now(), updated_at = now() WHERE run_id = %s AND name = %s",
                (run_id, step_name)
            )
            # Check if all steps are completed, if so mark run completed
            cur = await conn.execute(
                "SELECT COUNT(*) FROM steps WHERE run_id = %s AND status != 'completed'",
                (run_id,)
            )
            row = await cur.fetchone()
            if row and row[0] == 0:
                await conn.execute(
                    "UPDATE runs SET status = 'completed', finished_at = now(), updated_at = now() WHERE id = %s",
                    (run_id,)
                )

    async def mark_pending(self, step_id: str) -> None:
        """Reset a step back to pending if refused by a driver."""
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE steps SET status = 'pending', updated_at = now() WHERE id = %s",
                (step_id,)
            )
