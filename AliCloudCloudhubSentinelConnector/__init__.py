from __future__ import print_function
from aliyun.log import *
import os
import datetime
import logging
from datetime import datetime
import re
import azure.functions as func
# from .state_manager_async import StateManagerAsync
# from .sentinel_connector_async import AzureSentinelMultiConnectorAsync
from state_manager_async import StateManagerAsync
from sentinel_connector_async import AzureSentinelMultiConnectorAsync
import asyncio
import aiohttp
import time
# from azure.storage.blob.aio import ContainerClient

endpoint = os.environ.get('Endpoint', 'cn-hangzhou.log.aliyuncs.com')
accessKeyId = os.environ.get('AliCloudAccessKeyId', '')
accessKey = os.environ.get('AliCloudAccessKey', '')
token = ""
topic = os.environ.get('Topic', '')
user_projects = os.environ.get("AliCloudProjects").replace(" ", "").split(',')
customer_id = os.environ['WorkspaceID']
shared_key = os.environ['WorkspaceKey']
log_type = "AliCloud"
connection_string = os.environ['AzureWebJobsStorage']
chunksize = 2000
logAnalyticsUri = os.environ.get('logAnalyticsUri')

# if ts of last event is older than now - MAX_PERIOD_MINUTES -> script will get events from now - MAX_PERIOD_MINUTES
max_period_minutes = 60 * 24 * 7

# Defines how many files can be processed simultaneously
MAX_CONCURRENT_PROCESSING_FILES = int(os.environ.get('MAX_CONCURRENT_PROCESSING_FILES', 20))

# Defines page size while listing files from blob storage. New page is not processed while old page is processing.
MAX_PAGE_SIZE = int(MAX_CONCURRENT_PROCESSING_FILES * 1.5)

# Defines max number of events that can be sent in one request to Azure Sentinel
MAX_BUCKET_SIZE = int(os.environ.get('MAX_BUCKET_SIZE', 2000))

if ((logAnalyticsUri in (None, '') or str(logAnalyticsUri).isspace())):
    logAnalyticsUri = 'https://' + customer_id + '.ods.opinsights.azure.com'

pattern = r'https:\/\/([\w\-]+)\.ods\.opinsights\.azure.([a-zA-Z\.]+)$'
match = re.match(pattern, str(logAnalyticsUri))
if (not match):
    raise Exception("Ali Cloud: Invalid Log Analytics Uri")


# async def main(mytimer: func.TimerRequest) -> None:
#     if mytimer.past_due:
#         logging.info('The timer is past due!')
async def main() -> None:
    logging.getLogger().setLevel(logging.INFO)
    logging.info('Starting program')
    # logging.info(
    #     'Concurrency parameters: MAX_CONCURRENT_PROCESSING_FILES {}, MAX_PAGE_SIZE {}, MAX_BUCKET_SIZE {}.'.format(
    #         MAX_CONCURRENT_PROCESSING_FILES, MAX_PAGE_SIZE, MAX_BUCKET_SIZE))

    try:
        async with aiohttp.ClientSession() as session:
            async with aiohttp.ClientSession() as session_sentinel:
                Aliconn = AliCloudConnector(session=session, session_sentinel=session_sentinel)
                client = LogClient(endpoint, accessKeyId, accessKey, token)

                if user_projects == ['']:
                    projects = client.list_project(size=-1).get_projects()
                    project_names = list(map(lambda project_name: project_name["projectName"], projects))
                else:
                    project_names = user_projects

                tasks = []

                for project in project_names:
                    tasks.append(Aliconn.process_project(client, project))

                await asyncio.gather(*tasks)

        logging.info('Program finished. {} events have been sent.'.format(Aliconn.sentinel.successfull_sent_events_number))
   # except Exception as err:
    #    logging.error("Something wrong. Exception error text: {}".format(err))
    except BufferError as err:
        pass


class AliCloudConnector:
    def __init__(self, session: aiohttp.ClientSession, session_sentinel: aiohttp.ClientSession, max_concurrent_processing_files=10):
        self.user_projects = user_projects
        self.__access_key_id = accessKeyId
        self.__access_key = accessKey
        self.session = session
        self.session_sentinel = session_sentinel
        self._auth_lock = asyncio.Lock()
        self.semaphore = asyncio.Semaphore(max_concurrent_processing_files)
        self.logs_state_manager = StateManagerAsync(connection_string, share_name='alicloudcheckpoint',
                                                         file_path='alicloudlastlog')
        self.sentinel = AzureSentinelMultiConnectorAsync(self.session_sentinel, logAnalyticsUri, customer_id,
                                                         shared_key, queue_size=10000)
        self.sent_logs = 0
        self.last_ts = None
        self.total_blobs = 0
        self.total_events = 0

    # def _create_container_client(self):
    #     return ContainerClient.from_connection_string(self.__conn_string, self.__container_name, logging_enable=False,
    #                                                   max_single_get_size=2 * 1024 * 1024,
    #                                                   max_chunk_get_size=2 * 1024 * 1024)

    async def process_logstore(self, client, project, logstore):
        # last_log_ts_ms = await self.logs_state_manager.get()/1000
        last_log_ts_ms = 1650315600
        max_period = int(time.time()) - max_period_minutes * 60
        if not last_log_ts_ms or int(last_log_ts_ms) < max_period:
            log_start_ts_ms = max_period
            logging.info('Last log was too long ago or there is no info about last log timestamp.')
        else:
            log_start_ts_ms = int(last_log_ts_ms) + 1
        logging.info('Starting searching logs from {}'.format(log_start_ts_ms))

        async for event in self.get_logstore_logs(client, project, logstore, start_time=log_start_ts_ms):
            if not last_log_ts_ms:
                last_log_ts_ms = event['timestamp']
            elif event['timestamp'] > int(last_log_ts_ms):
                last_log_ts_ms = event['timestamp']
            await self.sentinel.send(event, log_type=log_type)
            self.sent_logs += 1

        self.last_ts = last_log_ts_ms

        conn = self.sentinel.get_log_type_connector(log_type)
        if conn:
            await conn.flush()
            logging.info('{} logs have been sent'.format(self.sent_logs))
        await self.save_checkpoint()

    async def get_logstores(self, client, project):
        request = ListLogstoresRequest(project)
        return client.list_logstores(request).get_logstores()

    async def process_project(self, client, project):
        tasks = []

        logstores = await self.get_logstores(client, project)
        for logstore in logstores:
            tasks.append(self.process_logstore(client, project, logstore))

        await asyncio.gather(*tasks)

    async def get_logstore_logs(self, client, project, logstore, start_time):
        end_time = int(time.time()) - 10
        print(project, logstore)
        res = client.get_log_all(project, logstore, start_time, end_time, topic)
        for logs in res:
            for log in logs.get_logs():
                yield {"timestamp": log.timestamp, "source": log.source, "contents": log.contents}

    async def save_checkpoint(self):
        if self.last_ts:
            # await self.logs_state_manager.post(str(self.last_ts))
            logging.info('Last ts saved - {}'.format(self.last_ts))


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
