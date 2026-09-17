import asyncio
import logging
from typing import Set

from .bus import Bus, StepCommand, StepResult
from .store import Store

log = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, store: Store, bus: Bus) -> None:
        self.store = store
        self.bus = bus
        self._lock = asyncio.Lock()

    async def start(self, run_id: str) -> None:
        """Begin executing a run."""
        async with self._lock:
            try:
                await self.store.start_run(run_id)
                log.info("scheduler: run %s started, evaluating initial steps", run_id)
                await self._evaluate_and_dispatch(run_id)
            except Exception:
                log.exception("Exception occurred in scheduler.start for run %s", run_id)
                raise

    async def handle_result(self, result: StepResult) -> None:
        """Record that a driver finished a step and move the run on."""
        async with self._lock:
            try:
                if getattr(result, "error", None):
                    log.error("scheduler: step %s failed with error: %s", result.step_name, result.error)
                    steps = await self.store.get_steps(result.run_id)
                    step_obj = next((s for s in steps if s.name == result.step_name), None)
                    step_id = step_obj.id if step_obj else ""
                    await self.store.set_workflow_failed(result.run_id, step_id, result.error)
                    return

                log.info(
                    "scheduler: driver %s reported %s finished",
                    result.device_id,
                    result.step_name,
                )

                await self.store.mark_completed(result.run_id, result.step_name)
                await self._evaluate_and_dispatch(result.run_id)
            except Exception:
                log.exception("Exception occurred in scheduler.handle_result for step %s", result.step_name)
                raise

    async def _evaluate_and_dispatch(self, run_id: str) -> None:
        if await self.store.is_failed(run_id):
            return

        steps = await self.store.get_steps(run_id)

        completed: Set[str] = {s.name for s in steps if s.status == "completed"}
        running: Set[str] = {s.name for s in steps if s.status == "dispatched"}

        log.info("Evaluating DAG for run %s. Completed: %s, Running: %s", run_id, completed, running)

        for step in steps:
            step_name = step.name

            if step_name in completed or step_name in running:
                continue

            dependencies = set(step.depends_on or [])
            if dependencies.issubset(completed):
                device = step.device_id

                if not await self.store.is_device_busy(device):
                    await self.store.record_step_dispatched(step.id)
                    log.info("scheduler: dispatching step %s to device %s", step_name, device)

                    try:
                        ack = await self.bus.send_command(
                            StepCommand(
                                run_id=run_id,
                                step_id=step.id,
                                step_name=step_name,
                                device_id=device,
                            )
                        )

                        if ack and not getattr(ack, "accepted", True):
                            log.warning(
                                "scheduler: device %s refused step %s, rolling back to pending: %s",
                                device,
                                step_name,
                                ack.reason,
                            )
                            await self.store.mark_pending(step.id)
                    except Exception as exc:
                        log.error("scheduler: failed to dispatch step %s to device %s: %r", step_name, device, exc)
                        await self.store.mark_pending(step.id)
