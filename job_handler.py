import functools

from redis import Redis
from rq import Queue
from rq.registry import StartedJobRegistry

import combined_lists
import notify_email


class JobHandler:
    """
    Manage list generation run on this machine.
    """
    def __init__(self, asyncio_loop):
        self.loop = asyncio_loop
        self.setup_job_queues()

    def setup_job_queues(self):
        """ Setup rq queues for submitting list generation and email notification jobs. """
        self.conn = Redis('localhost', 6379)
        self.generate_queue = Queue('generate', connection=self.conn, default_timeout="1h")
        self.email_queue = Queue('notify_email', connection=self.conn)

    async def submit_generate_job(self, config, list_id):
        """ Submit a new job for generating a list (with the given config) """
        if list_id not in await self.loop.run_in_executor(None, self.current_jobs):
            await self.loop.run_in_executor(None, functools.partial(self.generate_queue.enqueue, combined_lists.generate_combined_list, args=(config, list_id), job_id=str(list_id), timeout="1h"))
            return True
        else:
            return False

    async def submit_email_job(self, email_address, list_id, list_size):
        """ Submit a new job for sending an email once a list has been generated """
        generate_job = await self.loop.run_in_executor(None, self.generate_queue.fetch_job, list_id)
        await self.loop.run_in_executor(None, functools.partial(self.email_queue.enqueue, notify_email.send_notification_mailgun_api, email_address, list_id, list_size, depends_on=generate_job))
        return True

    def current_jobs(self):
        """ Track currently active and queued jobs """
        registry = StartedJobRegistry(queue=self.generate_queue)
        jobs = registry.get_job_ids() + self.current_jobs()

        return jobs

    def jobs_ahead_of_job(self, list_id):
        """ Count number of jobs ahead of current job """
        jobs = self.current_jobs()
        if list_id in jobs:
            return jobs.index(list_id)
        else:
            return 0

    async def get_job_status(self, list_id):
        """ Get current status of a job """
        job_success = await self.loop.run_in_executor(None, self.get_job_success, list_id)
        jobs_ahead = await self.loop.run_in_executor(None, self.jobs_ahead_of_job, list_id)
        return {"completed": job_success is not None, "jobs_ahead": jobs_ahead, "success": job_success}

    def get_job_success(self, list_id):
        """ Get current rq status of a job """
        return self.generate_queue.fetch_job(list_id).result


class JobHandlerRemote:
    """
    Manage relaying jobs to a remote machine that generates lists.
    """
    def __init__(self, asyncio_loop, endpoint=None, session=None):
        """

        :param asyncio_loop:
        :param endpoint: remote location that generates lists
        :param session: client session for aiohttp
        """
        if not endpoint or not session:
            raise ValueError
        self.endpoint = endpoint
        self.session = session

    async def submit_generate_job(self, config, list_id):
        """ Submit a new job for generating a list (with the given config) """
        async with self.session.post("{}/submit_generate".format(self.endpoint), json={"config": config, "list_id": list_id}) as response:
            jsn = await response.json()
            return jsn["success"]

    async def submit_email_job(self, email_address, list_id, list_size):
        """ Submit a new job for sending an email once a list has been generated """
        async with self.session.post("{}/submit_email".format(self.endpoint), json={"email_address": email_address, "list_id": list_id, "list_size": list_size}) as response:
            jsn = await response.json()
            return jsn["success"]

    async def get_job_status(self, list_id):
        """ Get current status of a job """
        async with self.session.get("{}/job_status".format(self.endpoint), params={"list_id": list_id}) as response:
            jsn = await response.json()
            return jsn

    async def retrieve_list(self, list_id, slice_size):
        """ Retrieve the contents of a remotely generated list """
        async with self.session.get("{}/retrieve_list".format(self.endpoint), json={"list_id": list_id, "slice_size": slice_size}) as response:
            while True:
                chunk = await response.content.read(1024)
                if not chunk:
                    break
                yield chunk
