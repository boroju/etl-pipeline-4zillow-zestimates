import argparse
from amherst_common.amherst_logger import AmherstLogger
import datadeliveryclient as ddc
import zestimates_codecs as codecs
import zestimates_pyodbc_sql as ddsql
import os
import json
import pandas as pd
import datetime as dt

# Settings
_TARGET_DB = 'IRSPublish'
_SCHEMA_NAME = 'dbo'
_TABLE_NAME = 'asg_PM_Zestimates_dataWrapper'


def parse_args():
    """
    Argument parsing function
    :return: Namespace containing all of the command line arguments
    """
    # Setup argument parsing
    parser = argparse.ArgumentParser(
        description='Python program to query Zestimates API and put data into SQL Server', add_help=True)
    parser.add_argument('-e', '--api_host', type=str, required=False,
                        help='API Host - e.g. "https://api.bridgedataoutput.com"',
                        default='https://api.bridgedataoutput.com')
    parser.add_argument('-k', '--access_token', type=str, required=True, help='Access Token')
    parser.add_argument('-s', '--sql_server', type=str, required=True, help='SQL Server instance where data is going '
                                                                            'to be placed')
    parser.add_argument('-t', '--state', type=str, required=True, help='State - e.g. "TX"')
    parser.add_argument('-c', '--city', type=str, required=True, help='City - e.g. "Austin"')
    parser.add_argument('-r', '--street', type=str, required=True, help='Street Name - e.g. "Tree Line"')
    parser.add_argument('-a', '--street_number', type=str, required=True, help='Street Number - e.g. "12613"')
    parser.add_argument('-z', '--zip_code', type=str, required=True, help='Zip Code - e.g. "78729"')
    parser.add_argument('-l', '--log', type=str, required=False, help='Specify path to the log directory',
                        default='/etl/log/')
    parser.add_argument('-g', '--debug', action='store_true', required=False, help='Specify log level as DEBUG')
    parsed_args = parser.parse_args()

    return parsed_args


def get_zestimates_json_object(bundles):
    # data_set = {'Zestimates': []}
    data_obj = []

    for b in bundles:
        data_obj.append(vars(b))

    # data_set['Zestimates'] = data_obj
    json_data = json.dumps(data_obj, cls=codecs.ZestimatesBundlesEncoder, indent=2)
    # return json string
    return json_data


def check_max_reference_id(obj_sql: ddsql.DataDeliverySQLServer, table_name):
    dbcur = obj_sql.cursor_to_sql_db()
    dbcur.execute("""
        SELECT MAX(referenceId) 
        FROM [dbo].[{0}]
        """.format(table_name.replace('\'', '\'\'')))

    max_ref_id = dbcur.fetchone()[0]
    dbcur.close()

    return max_ref_id


def check_if_table_exists(obj_sql: ddsql.DataDeliverySQLServer, table_name):
    dbcur = obj_sql.cursor_to_sql_db()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(table_name.replace('\'', '\'\'')))

    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True

    dbcur.close()
    return False


def get_new_reference_id(obj_sql: ddsql.DataDeliverySQLServer):
    max_ref_id = 0
    table_exists = check_if_table_exists(obj_sql, _TABLE_NAME)

    if table_exists:
        max_ref_id = check_max_reference_id(obj_sql, _TABLE_NAME)

    return int(max_ref_id) + 1


def load_dataframe_into_sqltable(dataframe, table, conn, schema, reference_id):
    dataframe["requestTimestamp"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dataframe["referenceId"] = reference_id
    # Load the Data in DataFrame into Table
    dataframe.to_sql(name=table, con=conn, schema=schema, if_exists='append', index=False)
    log.info(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' | Data Imported Successfully')


if __name__ == '__main__':
    # get arguments
    args = parse_args()
    zestimates_data_flag = False

    py_file = os.path.splitext(os.path.split(__file__)[1])[0]
    log = AmherstLogger(log_directory=args.log, log_file_app=py_file, vendor_name='Bridge',
                        vendor_product='Zestimates')

    # create a new object of DataDeliveryClient class
    ddc_obj = ddc.DataDeliveryClient(args.api_host, args.access_token)

    if args.city and args.state and args.street and args.street_number and args.zip_code:
        log.info('Getting zestimates data for provided parameters -> State, City, StreetName, StreetNumber, and zipCode')
        zbundles = ddc_obj.get_zestimates_by_address_parts(state=args.state, city=args.city,
                                                           street=args.street, street_number=args.street_number,
                                                           postal_code=args.zip_code)
        zestimates_data_flag = True
    else:
        log.info('You must complete State, City, Street Name, Street Number and zipCode')

    if zestimates_data_flag:
        # create a new Zestimates JSON object
        json_bundles = get_zestimates_json_object(zbundles)
        log.info('Zestimates JSON object has been created')
        a_json = json.loads(json_bundles)
        # create a new Zestimates Dataframe
        df = pd.DataFrame.from_dict(a_json, orient="columns")
        log.info('Zestimates Dataframe has been created')

        # create a new object of DataDeliverySQLServer class
        sql_obj = ddsql.DataDeliverySQLServer(target_server=args.sql_server, target_db=_TARGET_DB,
                                              schema_name=_SCHEMA_NAME, table_name=_TABLE_NAME)
        log.info('Database connection has been established')

        # Get referenceId from SQL Table
        log.info('Getting referenceId from the SQL Server table...')
        ref_id = get_new_reference_id(sql_obj)

        # Load the Data in DataFrame into Table
        log.info('Loading Dataframe data into SQL Server table...')
        load_dataframe_into_sqltable(dataframe=df, table=sql_obj.table_name, conn=sql_obj.conn_str,
                                     schema=sql_obj.schema_name, reference_id=ref_id)
    else:
        log.info('Error while getting Zestimates data from API')
