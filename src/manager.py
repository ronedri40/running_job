import asyncio
import logging
import inspect
from typing import Dict, List, Optional
from .models import Job, JobStatus, AgentConfig, Item
from .executor import AgentExecutor

logger = logging.getLogger(__name__)

class JobManager:
    def __init__(self):
        self.jobs: Dict[str, Job] = {}
        self.active_job_ids: List[str] = []
        self.active_batches_per_agent: Dict[str, int] = {}
        self.executor = AgentExecutor()
        self.running = False
        self._dispatcher_task: Optional[asyncio.Task] = None

    def submit_job(self, 
                   agent_name: str, 
                   config: AgentConfig, 
                   items: List[Item],
                   on_batch_complete: Optional[callable] = None,
                   on_job_complete: Optional[callable] = None) -> str:
        
        job_id = str(len(self.jobs) + 1) # Simple ID generation
        job = Job(
            job_id=job_id, 
            agent_name=agent_name, 
            config=config, 
            items_queue=list(items), # make a copy
            on_batch_complete=on_batch_complete,
            on_job_complete=on_job_complete
        )
        
        self.jobs[job_id] = job
        self.active_job_ids.append(job_id)
        logger.info(f"Job {job_id} submitted with {len(items)} items.")
        
        # Ensure dispatcher is running
        if not self.running:
            self.start_dispatcher()
            
        return job_id

    def cancel_job(self, job_id: str):
        job = self.jobs.get(job_id)
        if job and job.status not in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            job.status = JobStatus.CANCELLED
            logger.info(f"Job {job_id} cancelled.")
            # We don't remove from active_job_ids immediately, let dispatcher handle cleanup
            if job.on_job_complete:
                 asyncio.create_task(self._safe_callback(job.on_job_complete, job.job_id, job.to_dict()))

    def get_job_status(self, job_id: str) -> Optional[Dict]:
        job = self.jobs.get(job_id)
        return job.to_dict() if job else None

    def start_dispatcher(self):
        if not self.running:
            self.running = True
            self._dispatcher_task = asyncio.create_task(self._dispatcher_loop())
            logger.info("Dispatcher started.")

    async def stop_dispatcher(self):
        self.running = False
        if self._dispatcher_task:
            await self._dispatcher_task

    async def _safe_callback(self, callback, *args):
        if not callback: return
        try:
            if inspect.iscoroutinefunction(callback):
                await callback(*args)
            else:
                callback(*args)
        except Exception as e:
            logger.error(f"Error in callback: {e}")

    async def _dispatcher_loop(self):
        idx = 0
        while self.running:
            if not self.active_job_ids:
                await asyncio.sleep(0.1)
                continue

            # Round Robin Logic
            # Get current job from the cycle
            if idx >= len(self.active_job_ids):
                idx = 0
            
            job_id = self.active_job_ids[idx]
            job = self.jobs.get(job_id)
            
            # --- Cleanup & Status Checks ---
            if not job or job.status == JobStatus.CANCELLED:
                # Remove from active list
                self.active_job_ids.pop(idx)
                if job:
                     # ensure 'stopped' state if it was running?
                     pass
                continue
            
            if len(job.items_queue) == 0 and job.active_batches == 0:
                # Job is DONE
                job.status = JobStatus.COMPLETED
                logger.info(f"Job {job_id} completed.")
                if job.on_job_complete:
                    asyncio.create_task(self._safe_callback(job.on_job_complete, job_id, job.to_dict()))
                
                self.active_job_ids.pop(idx)
                continue

            if len(job.items_queue) == 0:
                # No more items to send, just waiting for batches to finish
                idx += 1
                continue

            # --- Dispatch Logic ---
            # Check GLOBAL Agent Concurrency Limit
            # We assume the current job's config carries the authoritative concurrency limit for this agent
            agent_concurrency = self.active_batches_per_agent.get(job.agent_name, 0)
            
            if agent_concurrency < job.config.concurrency:
                # We can dispatch a batch
                batch_size = job.config.batch_size
                batch = job.items_queue[:batch_size]
                job.items_queue = job.items_queue[batch_size:] # Remove from queue
                
                job.status = JobStatus.RUNNING
                job.active_batches += 1
                
                # Update global agent counter
                self.active_batches_per_agent[job.agent_name] = agent_concurrency + 1
                
                # Launch task
                asyncio.create_task(self._process_batch_wrapper(job, batch))
                
                # After dispatching ONE batch for this job, we yield to the next job (Fairness)
                idx += 1 
            else:
                # This agent is maxed out GLOBALLLY. 
                # Even if this specific job has few active batches, we cannot send more to this agent.
                # Skip to next job to see if it uses a DIFFERENT agent or if same agent frees up later.
                idx += 1 

            
            # Yield control to event loop to let other tasks run
            await asyncio.sleep(0)

    async def _process_batch_wrapper(self, job: Job, batch: List[Item]):
        try:
            results = await self.executor.process_batch(job.agent_name, job.config, batch)
            
            # Update stats
            success_count = sum(1 for r in results if r.get("status") != "error")
            failed_count = len(results) - success_count
            
            job.processed_count += success_count
            job.failed_count += failed_count
            
            # Trigger callback
            if job.on_batch_complete:
                await self._safe_callback(job.on_batch_complete, job.job_id, results)

        except Exception as e:
            logger.error(f"Unexpected error in batch execution for job {job.job_id}: {e}")
        finally:
            job.active_batches -= 1
            if job.agent_name in self.active_batches_per_agent:
                self.active_batches_per_agent[job.agent_name] -= 1
