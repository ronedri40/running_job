import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from src.manager import JobManager
from src.models import AgentConfig, Item, JobStatus

@pytest.mark.asyncio
async def test_round_robin_scheduling():
    manager = JobManager()
    
    # Mock executor
    manager.executor.process_batch = AsyncMock(return_value=[])

    # Config: Batch size 1, Concurrency 1 (Serial execution per job)
    config = AgentConfig(request_url="http://test", batch_size=1, concurrency=1)
    
    # Job A: 5 items
    items_a = [Item(item_id=f"a_{i}", item_type="t") for i in range(5)]
    # Job B: 5 items
    items_b = [Item(item_id=f"b_{i}", item_type="t") for i in range(5)]
    
    id_a = manager.submit_job("agent_a", config, items_a)
    id_b = manager.submit_job("agent_b", config, items_b)

    # Allow some time for processing
    # With strict Round Robin and concurrency 1, we expect batches to be submitted: A, B, A, B, ...
    
    # We can inspect the calls to process_batch
    # But since dispatcher runs in background, we need to wait a bit
    await asyncio.sleep(0.5) 
    
    # Check calls
    calls = manager.executor.process_batch.call_args_list
    assert len(calls) > 2
    
    # Verify interleaving (approximate)
    agent_names = [call.args[0] for call in calls]
    # We should see alternation
    # e.g. ['agent_a', 'agent_b', 'agent_a', 'agent_b'...]
    # Note: Timing might vary slightly, but we shouldn't see AAAAA... then BBBBB...
    
    has_alternation = False
    for i in range(len(agent_names) - 1):
        if agent_names[i] != agent_names[i+1]:
            has_alternation = True
            break
            
    assert has_alternation, "Jobs should be interleaved (Round Robin)"

    await manager.stop_dispatcher()

@pytest.mark.asyncio
async def test_concurrency_limit():
    manager = JobManager()
    
    # Mock executor to be slow
    async def slow_process(*args, **kwargs):
        await asyncio.sleep(0.2)
        return []
    
    manager.executor.process_batch = AsyncMock(side_effect=slow_process)

    # Config: Concurrency 2
    config = AgentConfig(request_url="http://test", batch_size=1, concurrency=2)
    items = [Item(item_id=f"item_{i}", item_type="t") for i in range(10)]
    
    job_id = manager.submit_job("agent_test", config, items)
    
    await asyncio.sleep(0.1) 
    
    job = manager.jobs[job_id]
    # Should have exactly 2 active batches (since processing is slow)
    assert job.active_batches <= 2
    
    await manager.stop_dispatcher()

@pytest.mark.asyncio
async def test_cancel_job():
    manager = JobManager()
    async def slow_mock(*args, **kwargs):
        await asyncio.sleep(0.01)
        return []
    manager.executor.process_batch = AsyncMock(side_effect=slow_mock)
    
    config = AgentConfig(request_url="http://test", batch_size=1, concurrency=1)
    items = [Item(item_id=f"item_{i}", item_type="t") for i in range(100)]
    
    job_id = manager.submit_job("agent_cancel", config, items)
    
    await asyncio.sleep(0.1)
    
    manager.cancel_job(job_id)
    
    assert manager.jobs[job_id].status == JobStatus.CANCELLED
    
    # Verify it doesn't process more
    start_processed = manager.jobs[job_id].processed_count
    await asyncio.sleep(0.2)
    end_processed = manager.jobs[job_id].processed_count
    
    # It might process one more batch that was already in flight, but not 10 more
    assert end_processed - start_processed <= 1 
    
    await manager.stop_dispatcher()

@pytest.mark.asyncio
async def test_callbacks():
    manager = JobManager()
    manager.executor.process_batch = AsyncMock(return_value=[{"status": "ok"}])
    
    callback_mock = AsyncMock()
    
    config = AgentConfig(request_url="http://test", batch_size=1, concurrency=1)
    items = [Item(item_id="1", item_type="t")]
    
    manager.submit_job("agent_cb", config, items, on_job_complete=callback_mock)
    
    await asyncio.sleep(0.5)
    
    assert manager.jobs["1"].status == JobStatus.COMPLETED
    callback_mock.assert_called_once()
    
    await manager.stop_dispatcher()
