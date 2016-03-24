#
# Copyright (c) 2015-2016 LabKey Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
############################################################################
NAME: 
LabKey Query API 

SUMMARY:  
This module provides functions for interacting with data on a LabKey Server.

DESCRIPTION:
This module is designed to simplify querying and manipulating data in LabKey Server.  
Its APIs are modeled after the LabKey Server JavaScript APIs of the same names. 

Documentation:
LabKey Python API:
https://www.labkey.org/wiki/home/Documentation/page.view?name=python

Setup, configuration of the LabKey Python API:
https://www.labkey.org/wiki/home/Documentation/page.view?name=setupPython

Using the LabKey Python API:
https://www.labkey.org/wiki/home/Documentation/page.view?name=usingPython

Documentation for the LabKey client APIs:
https://www.labkey.org/wiki/home/Documentation/page.view?name=viewAPIs

Support questions should be directed to the LabKey forum:
https://www.labkey.org/announcements/home/Server/Forum/list.view?


############################################################################
"""
from __future__ import unicode_literals

import pandas
from requests.exceptions import SSLError
from labkey.utils import build_url, handle_response
from labkey.exceptions import ServerContextError, RequestError, RequestAuthorizationError, QueryNotFoundError, ServerNotFoundError

_default_timeout = 60 * 5  # 5 minutes


def execute_sql(server_context, schema_name, sql, container_path=None,
                max_rows=None,
                sort=None,
                offset=None,
                container_filter=None,
                save_in_session=None,
                parameters=None,
                required_version=None,
                timeout=_default_timeout):
    """
    Execute sql query against a LabKey server.

    :param server_context: A LabKey server context. See utils.create_server_context.
    :param schema_name: schema of table
    :param sql: String of labkey sql to execute
    :param container_path: labkey container path if not already set in context
    :param max_rows: max number of rows to return
    :param sort: comma separated list of column names to sort by
    :param offset: number of rows to offset results by
    :param container_filter: enumeration of the various container filters available. See:
        https://www.labkey.org/download/clientapi_docs/javascript-api/symbols/LABKEY.Query.html#.containerFilter
    :param save_in_session: save query result as a named view to the session
    :param parameters: parameter values to pass through to a parameterized query
    :param required_version: Api version of response
    :param timeout: timeout of request in seconds (defaults to 30s)
    :return:
    """
    url = build_url(server_context, 'sql', 'execute.api', container_path=container_path)

    payload = {
        'schemaName': schema_name,
        'sql': sql,
        'sep': '\x1F\t',    # unit separator (US), tab (TAB)
        'eol': '\x1E\n'     # record separator (RS), line feed (LF)
    }

    if container_filter is not None:
        payload['containerFilter'] = container_filter

    if max_rows is not None:
        payload['maxRows'] = max_rows

    if offset is not None:
        payload['offset'] = offset

    if sort is not None:
        payload['query.sort'] = sort

    if save_in_session is not None:
        payload['saveInSession'] = save_in_session

    if parameters is not None:
        payload['query.parameters'] = parameters

    if required_version is not None:
        payload['apiVersion'] = required_version

    execute_sql_response = _make_request(server_context, url, payload, timeout=timeout)
    return execute_sql_response


def _make_request(server_context, url, payload, headers=None, timeout=_default_timeout):
    try:
        session = server_context['session']
        raw_response = session.post(url, data=payload, headers=headers, timeout=timeout)
        return _handle_response(raw_response)
    except SSLError as e:
        raise ServerContextError(e)


def _handle_response(response):
    sc = response.status_code

    if (200 <= sc < 300) or sc == 304:
        # map didn't work how I expected
        # result = map(lambda line: line.split("\x1F\t"), str(response.content).split("\x1E\n"))
        result = []
        for line in response.content.decode("utf-8").split("\x1E\n"):
            if 0 < len(line):
                result.append(line.split("\x1F\t"))
        columns = result[0]
        types = result[1]
        data = result[2:]
        df = pandas.DataFrame.from_records(data=data, columns=columns)
        print(df)
        for col in range(0,len(columns)):
            name = columns[col]
            typename = types[col]
            if typename == 'BOOLEAN' or typename == 'TINYINT' or typename == 'SMALLINT' or typename == 'INTEGER':
                df[[name]] = df[[name]].applymap(lambda x: None if (x is None or x == '') else int(x))
            elif typename == 'DOUBLE' or typename == 'REAL' or typename == 'NUMERIC':
                df[[name]] = df[[name]].applymap(lambda x: None if (x is None or x == '') else float(x))
            elif typename == 'TIMESTAMP':
                df[[name]] =  df[[name]].applymap(lambda x: None if (x is None or x == '') else pandas.to_datetime(x))
        return df
    elif sc == 401:
        raise RequestAuthorizationError(response)
    elif sc == 404:
        try:
            response.json()  # attempt to decode response
            raise QueryNotFoundError(response)
        except ValueError:
            # could not decode response
            raise ServerNotFoundError(response)
    else:
        raise RequestError(response)