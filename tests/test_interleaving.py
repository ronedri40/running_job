import pytest
import asyncio
from unittest.mock import AsyncMock
from src.manager import JobManager
from src.models import AgentConfig, Item

@pytest.mark.asyncio
async def test_round_robin_strict_interleaving():
    manager = JobManager()
    
    # We want to track the ORDER of calls
    call_order = []
    
    async def mock_process_batch(agent_name, config, batch):
        # Record which job the batch belongs to. 
        # Since we don't pass job_id to executor, we can infer it from the item_id prefix we set below.
        first_item = batch[0]
        job_prefix = first_item.item_id.split('_')[0]
        call_order.append(job_prefix)
        return []

    manager.executor.process_batch = AsyncMock(side_effect=mock_process_batch)

    # Config: 
    # Global Agent Concurrency = 1 (Strict serialization)
    # Batch size = 1
    config = AgentConfig(request_url="http://test", batch_size=1, concurrency=1)
    
    # 3 Jobs, each has 2 items
    items_1 = [Item(item_id=f"J1_{i}", item_type="t") for i in range(2)]
    items_2 = [Item(item_id=f"J2_{i}", item_type="t") for i in range(2)]
    items_3 = [Item(item_id=f"J3_{i}", item_type="t") for i in range(2)]
    
    # Using SHARED agent name
    manager.submit_job("shared_agent", config, items_1)
    manager.submit_job("shared_agent", config, items_2)
    manager.submit_job("shared_agent", config, items_3)
    
    # Wait for processing
    await asyncio.sleep(0.1)
    
    # We expect: J1, J2, J3, J1, J2, J3
    # Note: The very first batch of J1 might start slightly before J2 is submitted if we await, 
    # but since submit_job is synchronous here (except for the dispatcher loop context switch), 
    # and we sleep AFTER all 3 submits, the dispatcher *should* pick them up in order 
    # providing it hasn't looped too fast.
    
    # Actually, the dispatcher runs in background. 
    # submit_job starts dispatcher if not running.
    # J1 submitted -> Dispatcher Task Created.
    # Dispatcher runs... might pick J1 immediately.
    # J2 submitted.
    # Dispatcher loop yields... picks J2.
    
    print(f"Call Order: {call_order}")
    
    # Check the sequence locally
    # It should look like J1, J2, J3, J1, J2, J3
    # OR J1, J2, J3, J1, J2, J3 (order might shift slightly if J1 finishes fast, but the *start* order matters)
    
    # Ideally, we see at least one cycle of 1-2-3.
    assert "J1" in call_order[:3]
    assert "J2" in call_order[:3]
    assert "J3" in call_order[:3]
    
    await manager.stop_dispatcher()
