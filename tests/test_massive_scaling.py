import pytest
import asyncio
from unittest.mock import AsyncMock
from src.manager import JobManager
from src.models import AgentConfig, Item

@pytest.mark.asyncio
async def test_massive_parallel_agents():
    manager = JobManager()
    
    # Mock executor that takes some time so we can observe saturation
    async def fast_mock(agent_name, config, batch):
        await asyncio.sleep(0.05) # 50ms
        return [{"status": "ok"} for _ in batch]
    
    manager.executor.process_batch = AsyncMock(side_effect=fast_mock)

    NUM_AGENTS = 10
    JOBS_PER_AGENT = 10
    CONCURRENCY_LIMIT = 2
    
    # We want to verify that for ANY agent, active batches never exceeds 2
    # We can do this by wrapping the mock to check the live state
    
    max_observed_concurrency = {f"Agent_{i}": 0 for i in range(NUM_AGENTS)}
    
    async def instrumented_mock(agent_name, config, batch):
        # Check instantaneous concurrency from manager
        current = manager.active_batches_per_agent.get(agent_name, 0)
        if current > max_observed_concurrency[agent_name]:
            max_observed_concurrency[agent_name] = current
        
        # Real assertion: if this is ever > limit, we failed logic
        assert current <= CONCURRENCY_LIMIT, f"{agent_name} exceeded limit! Value: {current}"
        
        await asyncio.sleep(0.01)
        return [{"status": "ok"} for _ in batch]

    manager.executor.process_batch = AsyncMock(side_effect=instrumented_mock)

    print(f"\nSubmitting {NUM_AGENTS * JOBS_PER_AGENT} jobs...")
    
    config = AgentConfig("http://mock", batch_size=1, concurrency=CONCURRENCY_LIMIT)
    
    for a in range(NUM_AGENTS):
        agent_name = f"Agent_{a}"
        for j in range(JOBS_PER_AGENT):
            items = [Item(f"{a}_{j}_{x}", "t") for x in range(5)] # 5 items per job
            manager.submit_job(agent_name, config, items)

    # Let it run until all jobs complete
    # We can poll active_job_ids
    
    wait_cycles = 0
    while manager.active_job_ids:
        await asyncio.sleep(0.1)
        wait_cycles += 1
        if wait_cycles > 100: # 10 seconds timeout
            break
            
    assert len(manager.active_job_ids) == 0, "Not all jobs finished!"
    
    # double check our instrumented max
    for agent, peak in max_observed_concurrency.items():
        print(f"{agent} Peak Concurrency: {peak}")
        assert peak <= CONCURRENCY_LIMIT
        assert peak > 0 # It should have run something

    await manager.stop_dispatcher()
