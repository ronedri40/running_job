import asyncio
import logging
from src.manager import JobManager
from src.models import AgentConfig, Item
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("Experiment")

async def run_experiment(name, concurrency_limit, jobs_config):
    print(f"\n--- Running Experiment: {name} (Agent Concurrency: {concurrency_limit}) ---")
    
    manager = JobManager()
    
    # Mock executor with configurable delay
    async def mock_process(agent_name, config, batch):
        await asyncio.sleep(0.1) # 100ms per batch
        return [{"status": "ok"} for _ in batch]
    
    manager.executor.process_batch = mock_process
    
    start_time = time.time()
    
    active_ids = []
    for j_conf in jobs_config:
        config = AgentConfig(
            request_url="http://mock",
            batch_size=j_conf['batch'],
            concurrency=concurrency_limit # Global limit passed down via config (design assumption)
        )
        items = [Item(f"{j_conf['name']}_{i}", "t") for i in range(j_conf['items'])]
        jid = manager.submit_job("shared_agent", config, items)
        active_ids.append(jid)
        print(f"Submitted {j_conf['name']}: {j_conf['items']} items, Batch {j_conf['batch']}")

    # Monitor
    while True:
        all_done = True
        stats = []
        for jid in active_ids:
            s = manager.get_job_status(jid)
            if s['status'] not in ['COMPLETED', 'FAILED', 'CANCELLED']:
                all_done = False
            stats.append(f"J{jid}: {s['processed']}/{s['processed']+s['remaining']}")
        
        print(f"Time {time.time()-start_time:.1f}s | " + " | ".join(stats))
        
        if all_done:
            break
        await asyncio.sleep(0.5)

    duration = time.time() - start_time
    print(f"Experiment finished in {duration:.2f}s")
    await manager.stop_dispatcher()

async def main():
    # Experiment 1: Concurrency=3, 3 Jobs Equal Load
    # They should finish roughly at the same time
    await run_experiment(
        "3 Jobs, Concurrency 3", 
        concurrency_limit=3,
        jobs_config=[
            {'name': 'J1', 'items': 50, 'batch': 5},
            {'name': 'J2', 'items': 50, 'batch': 5},
            {'name': 'J3', 'items': 50, 'batch': 5},
        ]
    )

    # Experiment 2: Concurrency=3, 1 Big Job vs 2 Small Jobs
    # Small jobs should exit early thanks to Round Robin
    await run_experiment(
        "Fairness: 1 Big vs 2 Small (Concurrency 3)", 
        concurrency_limit=3,
        jobs_config=[
            {'name': 'Big', 'items': 100, 'batch': 5},   # 20 batches
            {'name': 'Small1', 'items': 10, 'batch': 5}, # 2 batches
            {'name': 'Small2', 'items': 10, 'batch': 5}, # 2 batches
        ]
    )
    
if __name__ == "__main__":
    asyncio.run(main())
