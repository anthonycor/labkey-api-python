# Python API for LabKey Server
<p>
 <a href="https://pypi.python.org/pypi/labkey"><img src="https://img.shields.io/pypi/v/labkey.svg" alt="pypi version"></a>
</p>
Lets you query, insert, and update data on a [LabKey Server](http://www.labkey.com/) using Python.

## Features

- **labkey.query.select_rows()** - Query and get results sets from LabKey Server.
- **labkey.query.execute_sql()** - Execute SQL (LabKey SQL dialect) through the query module on LabKey Server.
- **labkey.query.insert_rows()** - Insert rows into a table on LabKey Server.
- **labkey.query.update_rows()** - Update rows in a table on LabKey Server.
- **labkey.query.delete_rows()** - Delete records in a table on LabKey Server.
- **labkey.experiment.load_batch()** - Retreive assay data (batch level) from LabKey Server.
- **labkey.experiment.save_batch()** - Save assay data (batch level) on LabKey Server. 

## Installation
To install, simply use `pip`:

```bash
$ pip install labkey
```

**Note:** For users who installed this package before it was published to PyPI (before v0.3.0) it is recommended you uninstall and reinstall the package rather than attempting to upgrade. This is due to a change in the package's versioning semantics.

## Credentials
As of v0.4.0 this API no longer supports using a ``.labkeycredentials.txt`` file, and now uses the .netrc files similar to the other labkey APIs. Additional .netrc [setup instructions](https://www.labkey.org/wiki/home/Documentation/page.view?name=netrc) can be found at the link.

### Set Up a netrc File

On a Mac, UNIX, or Linux system the netrc file should be named ``.netrc`` (dot netrc) and on Windows it should be named ``_netrc`` (underscore netrc). The file should be located in your home directory and the permissions on the file must be set so that you are the only user who can read it, i.e. it is unreadable to everyone else.

To create the netrc on a Windows machine, first create an environment variable called ’HOME’ that is set to your home directory (for example, C:/Users/johndoe) or any directory you want to use.

In that directory, create a text file with the prefix appropriate to your system, either an underscore or dot.

The following three lines must be included in the file. The lines must be separated by either white space (spaces, tabs, or newlines) or commas:
```
machine <remote-instance-of-labkey-server>
login <user-email>
password <user-password>
```

For example:
```
machine mymachine.labkey.org
login user@labkey.org
password mypassword
```
Note that the netrc file only deals with connections at the machine level and should not include a port or protocol designation, meaning both "mymachine.labkey.org:8888" and "https://mymachine.labkey.org" are incorrect. 

## Examples

Sample code is available in the [samples](https://github.com/LabKey/labkey-api-python/tree/master/samples) directory.

The following gets data from the Users table on your local machine:

```python
from labkey.utils import create_server_context
from labkey.query import select_rows

print("Create a server context")
labkey_server = 'localhost:8080'
project_name = 'ModuleAssayTest'  # Project folder name
contextPath = 'labkey'
schema = 'core'
table = 'Users'

server_context = create_server_context(labkey_server, project_name, contextPath, use_ssl=False)

result = select_rows(server_context, schema, table)
if result is not None:
    print(result['rows'][0])
    print("select_rows: Number of rows returned: " + str(result['rowCount']))
else:
    print('select_rows: Failed to load results from ' + schema + '.' + table)
```

## Supported Versions
Python 2.6+ and 3.4+ are fully supported.
LabKey Server v13.3 and later.

## Contributing
This package is maintained by [LabKey](http://www.labkey.com/). If you have any questions or need support, please use the [LabKey Server support forum](https://www.labkey.org/wiki/home/page.view?name=support).

### Testing
If you are looking to contribute please run the tests before issuing a PR. For now you need to manually get the dependencies:

```bash
$ pip install mock
```

Then, to run the tests:

```bash
$ python test/test_labkey.py
```
