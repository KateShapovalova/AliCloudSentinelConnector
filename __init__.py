from __future__ import print_function
from aliyun.log import *
import time
import os


def get_list_logstores(client, project):
    request = ListLogstoresRequest(project)
    return client.list_logstores(request).get_logstores()


def get_logs(client, project, logstore, start_time="'2017-11-10 00:00:00'", end_time="'2017-11-13 00:00:00'"):
    req = GetProjectLogsRequest(project,
                                "select count(1) from %s where __date__ >%s and __date__ < %s" % (
                                    logstore, start_time, end_time))
    res = client.get_project_logs(req)
    return res.get_logs(), res.log_print()


def main():
    endpoint = os.environ.get('Endpoint', 'cn-hangzhou.log.aliyuncs.com')
    accessKeyId = os.environ.get('AccessKeyId', '')
    accessKey = os.environ.get('AccessKey', '')
    token = ""

    assert endpoint and accessKeyId and accessKey, ValueError("endpoint/access_id/key cannot be empty")

    client = LogClient(endpoint, accessKeyId, accessKey, token)

    projects = client.list_project(size=-1).get_projects()
    project_names = list(map(lambda project_name: project_name["projectName"], projects))
    print(project_names)
    for project in project_names:
        logstores = get_list_logstores(client, project)
        print(logstores)
        for logstore in logstores:
            logs = get_logs(client, project, logstore)
            for log in logs:
                print("timestamp:", log.timestamp, "source:", log.source, "contents:", log.contents)


if __name__ == '__main__':
    main()
