import datetime
import itertools
import time

import aiofiles
import aiohttp_jinja2
import aitertools
import jinja2
from aiohttp import web
from json import JSONDecodeError
import asyncio
import traceback

import combined_lists
import job_handler

from global_config import SERVER_PORT

DATE_FORMAT_WITH_HYPHEN = "%Y-%m-%d"

class Server:
    """ Web server for Tranco list """

    def __init__(self, loop):
        self.web_app = None
        self.server = None
        self.runner = None
        self.routes = web.RouteTableDef()
        self.loop = loop
        self.job_handler = job_handler.JobHandler()
        self.parse_download_list_size_cntr = 0

    async def multidict_to_dict(self, mdct):
        result = {}
        always_as_list = ["providers"]
        for k in mdct.keys():
            vs = mdct.getall(k)
            if len(vs) == 1 and k not in always_as_list:
                result[k] = vs[0]
            else:
                result[k] = vs
        return result

    async def get_date_interval_bounds(self, start_date, end_date, nb_days, nb_days_from):
        if start_date:
            start_date_dt = datetime.datetime.strptime(start_date, DATE_FORMAT_WITH_HYPHEN)
            return (start_date, (start_date_dt + datetime.timedelta(days=int(nb_days) - 1)).strftime(DATE_FORMAT_WITH_HYPHEN))
        elif end_date:
            end_date_dt = datetime.datetime.strptime(end_date, DATE_FORMAT_WITH_HYPHEN)
            return ((end_date_dt - datetime.timedelta(days=int(nb_days) - 1)).strftime(DATE_FORMAT_WITH_HYPHEN), end_date)

    async def parse_config_list_size(self, config):
        return config.get('listSizeCustomValue', config.get("listSize", None))

    async def process_config(self, config):
        """ Preprocess the incoming configuration to populate all necessary values correctly """
        config = await self.multidict_to_dict(config)
        list_size = await self.parse_config_list_size(config)
        del config["listSize"]
        config.pop("listSizeCustomValue", None)
        start_date, end_date = await self.get_date_interval_bounds(config.get('startDate', None), config.get('endDate', None), int(config.get('nbDays')), config.get('nbDaysFrom', None))
        config = {**config, "startDate": start_date, "endDate": end_date}
        return config, list_size

    async def generate_list(self, request, config=None, list_size=None):
        # Sets up a background job to generate the desired list; this page shows how this process
        # is progressing.
        if not config and not list_size:
            config, list_size = await self.process_config(await request.post())

        list_id = await self.loop.run_in_executor(None, combined_lists.config_to_list_id, config)

        if await self.loop.run_in_executor(None, combined_lists.list_available, list_id):
            return web.HTTPSeeOther("/list/{}/{}".format(list_id, list_size))
        else:
            await self.loop.run_in_executor(None, self.job_handler.submit_generate_job, config, list_id)
            return aiohttp_jinja2.render_template('generate_list.jinja2', request, {"list_id": list_id, "list_size": list_size}, status=202)

    async def poll_list_generation_status(self, request):
        # Tracks status of list generation, to be polled by the page for generating lists
        list_id = request.match_info['list_id']
        return web.json_response(await self.loop.run_in_executor(None, self.job_handler.get_job_status, list_id))

    async def notify_email(self, request):
        # Notify the requester by email that their list has been generated.

        post_data = await request.post()
        email_address = post_data["email"]
        list_id = post_data["list_id"]
        list_size = post_data["list_size"]

        await self.loop.run_in_executor(None, self.job_handler.submit_email_job, email_address, list_id, list_size)

        return web.FileResponse("./templates/notify_email.html", status=202)

    async def parse_download_list_size(self, list_size, use_none=True):
        try:
            self.parse_download_list_size_cntr += 1
            if list_size == "full":
                return None if use_none else "full"
            elif list_size.endswith("K"):
                return int(list_size[:-1]) * 1000
            else:
                return int(list_size)
        except:
            return None

    async def list_info(self, request):
        # An information page with an explanation of the options used to configure the list,
        # as well as a link to download the actual list.
        list_id = request.match_info['list_id']
        list_size = request.match_info['list_size']
        if await self.loop.run_in_executor(None, combined_lists.list_available, list_id):
            config = await self.loop.run_in_executor(None, combined_lists.list_id_to_config, list_id)
            config["list_size"] = list_size
            config["listSizeValue"] = await self.parse_download_list_size(list_size, use_none=False)
            response = aiohttp_jinja2.render_template('list_info.jinja2', request, config)
            return response
        else:
            return web.FileResponse("./templates/unavailable.html", status=404)

    async def retrieve_truncated_list(self, list_id, list_size):
        """ Get existing list from file """
        with open(await self.loop.run_in_executor(None, combined_lists.get_generated_list_fp, list_id), 'rb') as f:
            slice_size = await self.parse_download_list_size(list_size)
            for line in itertools.islice(f, slice_size):
                yield line

    async def download_list(self, request):
        # Downloads the actual list as a file.
        list_id = request.match_info['list_id']
        list_size = request.match_info['list_size']
        generator = self.retrieve_truncated_list(list_id, list_size)
        # TODO Compress result file?
        resp = web.Response(body=generator,
                            content_type="text/csv",
                            charset="utf-8",
                            headers={'Content-Disposition': 'Attachment;filename=thelist_{}.csv'.format(list_id)})
        resp.enable_compression()
        return resp

    async def download_latest(self, request):
        config = {"nbDays": "30", "nbDaysFrom": "end",
                  "combinationMethod": "dowdall", # TODO make choice based on assessment on stability etc.
                  "listPrefix": 'full',
                  "includeDomains": 'all', # TODO make choice
                  "filterPLD": "on",
                  }

        providers = ["alexa", "umbrella", "majestic", "quantcast"]
        provider = request.query['provider']
        if provider == "all":
            config["providers"] = providers
        elif provider in providers:
            config["providers"] = [provider]
        else:
            raise Exception

        yesterday = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime(DATE_FORMAT_WITH_HYPHEN)
        config["startDate"], config["endDate"] = await self.get_date_interval_bounds(None, yesterday, "30", "end")

        print(config)

        return await self.generate_list(request, config, request.query["list_size"])

    async def initialize_routes(self):
        self.web_app.add_routes([
            web.get('/', lambda r: web.FileResponse("./templates/index.html")),
            web.get('/methodology', lambda r: web.FileResponse("./templates/methodology.html")),
            web.get('/configure', lambda r: web.FileResponse("./templates/configure.html")),
            web.post('/generate_list', self.generate_list),
            web.get('/generate_list/{list_id}', self.poll_list_generation_status, allow_head=False),
            web.post('/notify_email', self.notify_email),
            web.get('/list/{list_id}', lambda r: web.HTTPSeeOther("/list/{}/full".format(r.match_info['list_id']))),
            web.get('/list/{list_id}/{list_size}', self.list_info),
            web.get('/download/{list_id}/{list_size}', self.download_list),
            web.get('/download_latest', self.download_latest),
            web.get('/failure', lambda r: web.FileResponse("./templates/failure.html", status=500)), # status is ignored for FileResponse
        ])
        self.web_app.router.add_static('/assets/',
                                       path="./assets",
                                       name='static')

    def shutdown(self):
        # https://aiohttp.readthedocs.io/en/stable/web.html#aiohttp-web-graceful-shutdown
        print("Shutting down server...")
        self.runner.cleanup()
        print("Server shut down.")

    async def run(self):
        self.web_app = web.Application()
        await self.initialize_routes()
        aiohttp_jinja2.setup(self.web_app,
                             loader=jinja2.FileSystemLoader('./templates'))
        self.runner = web.AppRunner(self.web_app)
        await self.runner.setup()
        self.server = web.TCPSite(self.runner, '0.0.0.0', SERVER_PORT)
        await self.server.start()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    server = Server(loop)
    loop.run_until_complete(server.run())
    loop.run_forever()
