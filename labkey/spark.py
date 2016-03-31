from __future__ import unicode_literals

import labkey
from pyspark.sql.types import *


def _sparkField_from_field(field):
    type = StringType()
    if field['jsonType'] == 'boolean':
        type = BooleanType()
    elif field['jsonType'] == 'int':
        type = IntegerType()
    elif field['jsonType'] == 'float':
        type = DoubleType()
    elif field['jsonType'] == 'date':
        type = StringType()
    return StructField(field['fieldKey'], type, field['isNullable'])


def _sparkType_from_fields(fields):
    list = []
    for field in fields:
        list.append(_sparkField_from_field(field))
    return StructType(list)


# translate JSON formatted dates to ISO format
def _fixup_dates(result):
    datecols = []
    for field in result['metaData']['fields']:
        if field['jsonType'] == 'date':
            datecols.append(field['name'])
    for datecol in datecols:
        for row in result['rows']:
            if row[datecol] is not None:
                row[datecol] = row[datecol].replace('/','-')


# TODO add full select_rows compatibility
def labkey_select_rdd(server_context, spark, spark_sql, schema, table, alias=None, container_path=None):
    result = labkey.query.select_rows(server_context, schema, table, container_path=container_path)
    _fixup_dates(result)
    if result is None: return None
    schema = _sparkType_from_fields(result['metaData']['fields'])
    df = spark_sql.createDataFrame(spark.parallelize(result['rows']), schema)
    if alias is not None:
        df.registerTempTable(alias)
    return df


def labkey_sql_rdd(server_context, spark, spark_sql, schema, sql, alias=None, container_path=None):
    pd = labkey.sql.execute_sql(server_context, schema, sql, container_path=container_path)
    df = spark_sql.createDataFrame(pd)
    if alias is not None:
        df.registerTempTable(alias)
    return df
