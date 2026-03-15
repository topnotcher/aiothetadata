"""Acceptance test configuration.

Acceptance tests run against a live thetaterm instance. Set the THETATERM
environment variable to the URL of your thetaterm server before running:

    THETATERM=http://thetaterm:25503 pytest acceptance/

The default URL is ``http://127.0.0.1:25503/``.
"""
import os
import pytest
from aiothetadata.client import ThetaClient


@pytest.fixture(scope='session')
async def client():
    url = os.getenv('THETATERM', 'http://127.0.0.1:25503/')
    async with ThetaClient(url) as c:
        yield c
