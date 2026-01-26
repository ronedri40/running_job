import asyncio
import logging
from aiohttp import web
from .manager import JobManager
from .models import AgentConfig, Item

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Server")

class JobServer:
    def __init__(self):
        self.manager = JobManager()
        self.app = web.Application()
        self.app.add_routes([
            web.post('/submit', self.handle_submit),
            web.post('/cancel/{job_id}', self.handle_cancel),
            web.get('/status/{job_id}', self.handle_status),
            web.get('/health', self.handle_health)
        ])
        self.app.on_startup.append(self.start_background_tasks)
        self.app.on_cleanup.append(self.cleanup_background_tasks)

    async def start_background_tasks(self, app):
        self.manager.start_dispatcher()

    async def cleanup_background_tasks(self, app):
        await self.manager.stop_dispatcher()

    async def handle_submit(self, request):
        try:
            data = await request.json()
            
            # Validation
            if "agent_name" not in data or "items" not in data:
                return web.json_response({"error": "Missing agent_name or items"}, status=400)

            # Parse Config
            config_data = data.get("config", {})
            config = AgentConfig(
                request_url=config_data.get("request_url", ""),
                batch_size=config_data.get("batch_size", 10),
                concurrency=config_data.get("concurrency", 2),
                timeout_ms=config_data.get("timeout_ms", 5000),
                retry_limit=config_data.get("retry_limit", 3),
                headers=config_data.get("headers", {})
            )

            # Parse Items
            items = []
            for i in data["items"]:
                items.append(Item(
                    item_id=i.get("item_id"),
                    item_type=i.get("item_type"),
                    payload=i.get("payload", {})
                ))

            # Optional Callbacks (Not over HTTP usually, but we could use webhooks in future)
            # For now, just logging
            
            job_id = self.manager.submit_job(data["agent_name"], config, items)
            
            return web.json_response({"job_id": job_id, "status": "submitted"})

        except Exception as e:
            logger.error(f"Error submitting job: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def handle_cancel(self, request):
        job_id = request.match_info['job_id']
        self.manager.cancel_job(job_id)
        return web.json_response({"job_id": job_id, "status": "cancelled"})

    async def handle_status(self, request):
        job_id = request.match_info['job_id']
        status = self.manager.get_job_status(job_id)
        if not status:
            return web.json_response({"error": "Job not found"}, status=404)
        return web.json_response(status)
    
    async def handle_health(self, request):
        return web.json_response({"status": "ok", "active_jobs": len(self.manager.active_job_ids)})

def run():
    server = JobServer()
    web.run_app(server.app, port=8080)

if __name__ == "__main__":
    run()
