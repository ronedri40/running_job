import pytest
import asyncio
from src.manager import JobManager
from src.models import AgentConfig, Item

@pytest.mark.asyncio
async def test_submission_race_condition():
    manager = JobManager()
    
    # We will bombard the manager with 50 concurrent submissions
    # If there was a race condition in "check running -> start dispatcher",
    # we might see multiple dispatchers or weird state (though asyncio is single threaded so it won't happen).
    
    async def submit_concurrently(i):
        config = AgentConfig(request_url="http://mock", batch_size=1, concurrency=1)
        items = [Item(f"item_{i}", "t")]
        # To simulate "network" concurrency before reaching the sync manager method, we sleep 0
        await asyncio.sleep(0) 
        return manager.submit_job(f"agent_{i}", config, items)

    # Launch 50 tasks at once
    tasks = [submit_concurrently(i) for i in range(50)]
    job_ids = await asyncio.gather(*tasks)
    
    # Assertions
    assert len(job_ids) == 50
    assert len(manager.jobs) == 50
    assert len(manager.active_job_ids) == 50
    
    # Ensure IDs are unique (1..50)
    assert len(set(job_ids)) == 50
    
    # Ensure dispatcher is running
    assert manager.running is True
    assert manager._dispatcher_task is not None
    assert not manager._dispatcher_task.done()

    await manager.stop_dispatcher()
