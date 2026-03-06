"""
Shared test utilities for aiothetadata tests.
"""
import io
import csv

from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, RawTestServer


def csv_response(header, rows):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(header)
    for row in rows:
        if isinstance(row, str):
            row = row.split(',')
        writer.writerow(row)
    return web.Response(text=output.getvalue(), content_type='text/csv')


class RequestHandler:
    def __init__(self, callback):
        self.requests = []
        self.callback = callback

    async def __call__(self, request):
        self.requests.append(request)
        return await self.callback(request)

    def assert_csv(self):
        for request in self.requests:
            assert request.query.get('format', 'csv') == 'csv'

    def get_params(self, n=0):
        assert len(self.requests) > n
        return dict(self.requests[n].query)


class Handler:
    def __init__(self):
        self.handlers = {}

    def register(self, path, handler):
        self.handlers[path] = RequestHandler(handler)
        return self.handlers[path]

    async def __call__(self, request):
        return await self.handlers[request.path](request)


class BaseThetaClientTest(AioHTTPTestCase):
    async def asyncSetUp(self):
        self.handler = Handler()
        self.server = RawTestServer(self.handler)
        await self.server.start_server()
        self.client = await self.get_client(self.make_url('/'))

    async def get_client(self, url):
        raise NotImplementedError

    async def asyncTearDown(self):
        await self.server.close()
        await self.client.close()

    def make_url(self, *args, **kwargs):
        return str(self.server.make_url(*args, **kwargs))
