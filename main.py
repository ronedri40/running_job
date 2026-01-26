import asyncio
import logging
from src.models import AgentConfig, Item
from src.manager import JobManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Main")

async def mock_server(request):
    """
    To properly test, we'd need a real server. 
    For this demo, we rely on the executor hitting a URL. 
    We can use httpbin.org or similar if we want real network calls, 
    or mock the AgentExecutor in tests.
    
    Here we will just assume the user runs a local server or we mock the executor method.
    """
    pass

# Mocking the executor's internal request method for the demo
# so we don't need an actual HTTP server running.
async def mock_send_request(session, config, payload, batch):
    await asyncio.sleep(0.5) # Simulate network latency
    logger.info(f"Agent {payload['agent_name']} processed batch of {len(batch)} items.")
    return [{"item_id": item['item_id'], "status": "success"} for item in payload['items']]

async def main():
    manager = JobManager()
    
    # Monkey patch executor for demo purposes
    manager.executor._send_request_with_retry = mock_send_request

    # Setup Callback
    def on_batch_done(job_id, results):
        print(f"Callback found: Job {job_id} finished a batch of {len(results)} items.")

    async def on_job_done(job_id, stats):
        print(f"Callback found: Job {job_id} COMPLETED. Stats: {stats}")

    # Job 1: Heavy Job (100 items)
    config_heavy = AgentConfig(
        request_url="http://localhost:8080/execute",
        batch_size=10,
        concurrency=2
    )
    items_heavy = [Item(item_id=f"heavy-{i}", item_type="type1") for i in range(100)]
    
    # Job 2: Light Job (10 items)
    config_light = AgentConfig(
        request_url="http://localhost:8080/execute",
        batch_size=2,
        concurrency=1
    )
    items_light = [Item(item_id=f"light-{i}", item_type="type2") for i in range(10)]

    logger.info("Submitting Heavy Job...")
    id1 = manager.submit_job("agent_heavy", config_heavy, items_heavy, on_batch_complete=on_batch_done)
    
    await asyncio.sleep(1) # Let it start
    
    logger.info("Submitting Light Job...")
    id2 = manager.submit_job("agent_light", config_light, items_light, on_job_complete=on_job_done)

    # Let them run for a bit
    while True:
        status1 = manager.get_job_status(id1)
        status2 = manager.get_job_status(id2)
        
        print(f"Status: Job1={status1['processed']}/{status1['processed']+status1['remaining']} | Job2={status2['processed']}/{status2['processed']+status2['remaining']}")
        
        if status1['status'] == 'COMPLETED' and status2['status'] == 'COMPLETED':
            break
        
        await asyncio.sleep(1)

    print("All jobs completed.")
    await manager.stop_dispatcher()

if __name__ == "__main__":
    asyncio.run(main())
