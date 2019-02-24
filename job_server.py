import asyncio
import aitertools
from aiohttp import web

import combined_lists
import job_handler
from global_config import JOB_SERVER_PORT


class JobServer:
    """ Job server for accepting requests for generating a custom Tranco list (hosted on remote machine) """

    def __init__(self, loop):
        self.web_app = None
        self.server = None
        self.runner = None
        self.routes = web.RouteTableDef()
        self.loop = loop
        self.job_handler: job_handler.JobHandler = None

    async def submit_generate_job(self, request):
        """ Submit a new job for generating a list (with the given config) """
        post_data = await request.json()
        print("Generating ", post_data)
        result = await self.job_handler.submit_generate_job(post_data["config"], post_data["list_id"])
        return web.json_response({"success": result})

    async def submit_email_job(self, request):
        """ Submit a new job for sending an email once a list has been generated """
        post_data = await request.json()
        result = await self.job_handler.submit_email_job(post_data["email_address"], post_data["list_id"], post_data["list_size"])
        return web.json_response({"success": result})

    async def get_job_status(self, request):
        """ Get current status of a job """
        list_id = request.query['list_id']
        print("Getting status for ", list_id)
        return web.json_response(await self.job_handler.get_job_status(list_id))

    async def retrieve_list(self, request):
        """ Retrieve the contents of a remotely generated list """
        post_data = await request.json()
        list_id = post_data["list_id"]
        slice_size = post_data["slice_size"]
        file_path = await self.loop.run_in_executor(None, combined_lists.get_generated_list_fp, list_id)

        async def generator():
            with open(file_path) as csvf:
                async for line in aitertools.islice(csvf, slice_size):
                    yield line.encode("utf-8")

        return web.Response(body=generator(),
                            content_type="text/csv",
                            charset="utf-8",
                            )

    async def initialize_routes(self):
        self.web_app.add_routes([
            web.post('/submit_generate', self.submit_generate_job),
            web.post('/submit_email', self.submit_email_job),
            web.get('/job_status', self.get_job_status),
            web.get('/retrieve_list', self.retrieve_list)
        ])

    async def run(self):
        self.job_handler = job_handler.JobHandler(self.loop)

        self.web_app = web.Application()

        await self.initialize_routes()
        self.runner = web.AppRunner(self.web_app)
        await self.runner.setup()
        self.server = web.TCPSite(self.runner, '0.0.0.0', JOB_SERVER_PORT)
        await self.server.start()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    server = JobServer(loop)
    loop.run_until_complete(server.run())
    loop.run_forever()