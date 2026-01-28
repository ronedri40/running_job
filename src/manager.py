import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Any, Set

@dataclass
class JobContext:
    job_id: str
    agent_name: str
    items: List[Any]
    params: Dict[str, Any]
    callbacks: Any
    batch_size: int
    url: str
    timeout: int
    next_batch_idx: int = 0
    total_batches: int = 0
    processed_items: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    done_evt: asyncio.Event = field(default_factory=asyncio.Event)

class Dispatcher:
    def _init_(self, agent_configs, http_engine: HttpEngine):
        self.configs = agent_configs
        self.http_engine = http_engine
        self.rotations: Dict[str, deque[JobContext]] = {name: deque() for name in agent_configs}
        self.semaphores: Dict[str, asyncio.Semaphore] = {
            name: asyncio.Semaphore(cfg.concurrency) for name, cfg in agent_configs.items()
        }
        self.cancel_flags: Set[str] = set()
        self._manager_tasks = []

    async def start(self):
        for name in self.configs:
            self._manager_tasks.append(asyncio.create_task(self._agent_manager(name)))

    async def stop(self):
        for t in self._manager_tasks: t.cancel()
        await asyncio.gather(*self._manager_tasks, return_exceptions=True)

    async def submit(self, ctx: JobContext):
        self.rotations[ctx.agent_name].append(ctx)

    async def _agent_manager(self, agent_name: str):
        sem = self.semaphores[agent_name]
        rotation = self.rotations[agent_name]
        while True:
            if not rotation:
                await asyncio.sleep(0.05) # Backoff
                continue

            await sem.acquire() # Acquire slot BEFORE starting work
            ctx = rotation.popleft()
            asyncio.create_task(self._run_batch_step(ctx, sem, rotation))

    async def _run_batch_step(self, ctx: JobContext, sem: asyncio.Semaphore, rotation: deque):
        batch_idx = -1
        try:
            # 1. Check Cancel & Assign Batch Index
            async with ctx.lock:
                if ctx.job_id in self.cancel_flags:
                    if not ctx.done_evt.is_set():
                        await ctx.callbacks.on_job_canceled(ctx.job_id, ctx.agent_name, ctx.processed_items, "Canceled")
                        ctx.done_evt.set()
                    sem.release() # Release if we don't proceed to HTTP
                    return
                
                batch_idx = ctx.next_batch_idx
                ctx.next_batch_idx += 1
                if ctx.next_batch_idx < ctx.total_batches:
                    rotation.append(ctx) # Re-queue for next worker to pick up in parallel

            # 2. HTTP Call (The Agent Work)
            start = batch_idx * ctx.batch_size
            items = ctx.items[start : start + ctx.batch_size]
            
            res = await self.http_engine.process_batch(ctx.url, {
                "job_id": ctx.job_id, "agent_name": ctx.agent_name,
                "batch_index": batch_idx, "items": items, "params": ctx.params
            }, ctx.timeout)

            # 3. RELEASE SEMAPHORE IMMEDIATELY after HTTP response
            # This allows the next batch to start hitting the agent while we update S3
            sem.release()

            # 4. State Update (S3 Callbacks)
            async with ctx.lock:
                ctx.processed_items += len(items)
                await ctx.callbacks.on_batch_done(ctx.job_id, ctx.agent_name, batch_idx, len(items), res)
                
                if ctx.processed_items >= len(ctx.items) and not ctx.done_evt.is_set():
                    await ctx.callbacks.on_job_done(ctx.job_id, ctx.agent_name, ctx.processed_items, "Complete")
                    ctx.done_evt.set()

        except Exception as e:
            # Handle failure ensuring semaphore is released if not already
            try:
                sem.release()
            except ValueError: pass # Already released
            
            async with ctx.lock:
                if not ctx.done_evt.is_set():
                    await ctx.callbacks.on_job_failed(ctx.job_id, ctx.agent_name, ctx.processed_items, str(e))
                    ctx.done_evt.set()
