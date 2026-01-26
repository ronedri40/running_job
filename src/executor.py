import asyncio
import aiohttp
import logging
import logging
from typing import List, Dict, Any, Optional
from .models import AgentConfig, Item

logger = logging.getLogger(__name__)

class AgentExecutor:
    """
    Handles the execution of a batch of items against an external agent via HTTP.
    """
    
    def __init__(self):
        pass

    async def process_batch(self, agent_name: str, config: AgentConfig, batch: List[Item]) -> List[Dict]:
        """
        Sends a batch of items to the agent.
        Returns a list of results (success or failure) for each item.
        """
        
        payload = {
            "agent_name": agent_name,
            "items": [
                {
                    "item_id": item.item_id,
                    "item_type": item.item_type,
                    "payload": item.payload
                } for item in batch
            ]
        }
        
        try:
            async with aiohttp.ClientSession(headers=config.headers) as session:
                return await self._send_request_with_retry(session, config, payload, batch)
        except Exception as e:
            logger.error(f"Critical error processing batch for agent {agent_name}: {str(e)}")
            # Fallback: mark all as failed
            return [
                {"item_id": item.item_id, "status": "error", "error": str(e)} 
                for item in batch
            ]

    async def _send_request_with_retry(self, session: aiohttp.ClientSession, config: AgentConfig, payload: Dict, batch: List[Item]) -> List[Dict]:
        attempt = 0
        last_error = None
        
        while attempt <= config.retry_limit:
            try:
                # Use timeout
                timeout = aiohttp.ClientTimeout(total=config.timeout_ms / 1000.0)
                
                async with session.post(config.request_url, json=payload, timeout=timeout) as response:
                    if response.status == 200:
                        data = await response.json()
                        if isinstance(data, list):
                             return data
                        return data.get("results", [])
                    
                    # If 5xx or 429, we retry. If 400, strictly fail?
                    # For simplicity, treating non-200 as retryable error for now, or just fail.
                    error_text = await response.text()
                    logger.warning(f"Agent returned {response.status}: {error_text}")
                    last_error = f"HTTP {response.status}"
            
            except asyncio.TimeoutError:
                logger.warning(f"Timeout (attempt {attempt+1}/{config.retry_limit + 1})")
                last_error = "Timeout"
            except Exception as e:
                logger.warning(f"Request failed (attempt {attempt+1}): {str(e)}")
                last_error = str(e)
            
            attempt += 1
            if attempt <= config.retry_limit:
                await asyncio.sleep(2 ** attempt) # Exponential backoff
        
        # If exhausted retries:
        logger.error(f"Exhausted retries for batch. Error: {last_error}")
        return [
            {"item_id": item.item_id, "status": "error", "error": f"Max retries reached. Last: {last_error}"} 
            for item in batch
        ]
