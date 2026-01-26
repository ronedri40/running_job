from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Callable
from enum import Enum
import uuid

class JobStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

@dataclass
class Item:
    item_id: str
    item_type: str
    payload: Dict[str, Any] = field(default_factory=dict)

@dataclass
class AgentConfig:
    request_url: str
    batch_size: int = 10
    concurrency: int = 2
    timeout_ms: int = 5000
    retry_limit: int = 3
    headers: Dict[str, str] = field(default_factory=dict)

@dataclass
class Job:
    job_id: str
    agent_name: str
    config: AgentConfig
    items_queue: List[Item] 
    
    # Callbacks
    on_batch_complete: Optional[Callable[[str, List[Dict]], None]] = None 
    on_job_complete: Optional[Callable[[str, Dict], None]] = None 

    status: JobStatus = JobStatus.PENDING
    processed_count: int = 0
    failed_count: int = 0
    
    # To track in-flight tasks for this job
    active_batches: int = 0

    def to_dict(self):
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "processed": self.processed_count,
            "failed": self.failed_count,
            "remaining": len(self.items_queue),
            "active_batches": self.active_batches
        }
