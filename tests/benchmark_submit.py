import asyncio
import time
from src.manager import JobManager
from src.models import AgentConfig, Item

async def benchmark():
    manager = JobManager()
    config = AgentConfig(request_url="http://mock", batch_size=1, concurrency=1)
    items = [Item("1", "t")]
    
    start = time.time()
    count = 10000
    
    print(f"Submitting {count} jobs...")
    
    # Just measuring pure submission speed
    for i in range(count):
        manager.submit_job(f"agent_{i}", config, items)
        
    duration = time.time() - start
    print(f"Submitted {count} jobs in {duration:.4f} seconds.")
    print(f"Throughput: {count / duration:.2f} jobs/second")
    
    # Clean up
    await manager.stop_dispatcher()

if __name__ == "__main__":
    asyncio.run(benchmark())
