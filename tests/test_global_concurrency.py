import pytest
import asyncio
from unittest.mock import AsyncMock
from src.manager import JobManager
from src.models import AgentConfig, Item

@pytest.mark.asyncio
async def test_global_agent_concurrency():
    manager = JobManager()
    
    # Mock executor to be slow so we can accrue concurrency
    async def slow_mock(*args, **kwargs):
        await asyncio.sleep(0.1)
        return []
    manager.executor.process_batch = AsyncMock(side_effect=slow_mock)

    # Config: Concurrency 2 GLOBALLY for this agent
    # We will submit 2 jobs for the SAME agent.
    # Job A inputs 10 items
    # Job B inputs 10 items
    # If Global Concurrency works, Total Active Batches for "shared_agent" should never exceed 2.
    # If it was Per-Job, it would reach 4 (2 for A + 2 for B).
    
    config = AgentConfig(request_url="http://test", batch_size=1, concurrency=2)
    
    items_a = [Item(item_id=f"a_{i}", item_type="t") for i in range(10)]
    items_b = [Item(item_id=f"b_{i}", item_type="t") for i in range(10)]
    
    id_a = manager.submit_job("shared_agent", config, items_a)
    id_b = manager.submit_job("shared_agent", config, items_b)
    
    # Wait for dispatcher to spin up some tasks
    await asyncio.sleep(0.05)
    
    total_active = manager.active_batches_per_agent.get("shared_agent", 0)
    
    # Assert
    assert total_active <= 2, f"Total active batches {total_active} exceeded global limit of 2"
    
    job_a = manager.jobs[id_a]
    job_b = manager.jobs[id_b]
    
    # Also verify they are sharing the slots (e.g. 1 and 1, or 2 and 0 transitionally)
    # But strictly Sum <= 2
    assert job_a.active_batches + job_b.active_batches <= 2
    
    await manager.stop_dispatcher()
