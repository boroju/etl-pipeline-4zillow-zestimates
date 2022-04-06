import argparse
from amherst_common.amherst_logger import AmherstLogger
import datadeliveryclient as ddc
import zestimates_codecs as codecs
import zestimates_pyodbc_sql as ddsql
import os
import json
import pandas as pd
import datetime as dt
import time

# Settings
_TARGET_DB = 'IRSPublish'
_SCHEMA_NAME = 'dbo'
_TABLE_NAME = 'ZEstimate_Monthly_RandomSample'


def parse_args():
    """
    Argument parsing function
    :return: Namespace containing all the command line arguments
    """
    # Setup argument parsing
    parser = argparse.ArgumentParser(
        description='Python program to query Zestimates Monthly Random Sample and put data into SQL Server',
        add_help=True)
    parser.add_argument('-e', '--api_host', type=str, required=False,
                        help='API Host - e.g. "https://api.bridgedataoutput.com"',
                        default='https://api.bridgedataoutput.com')
    parser.add_argument('-k', '--access_token', type=str, required=True, help='Access Token')
    parser.add_argument('-s', '--sql_server', type=str, required=True, help='SQL Server instance where data is going '
                                                                            'to be placed')
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


def load_dataframe_into_sqltable(dataframe, table, conn, schema, asg_prop_id):
    dataframe["requestTimestamp"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dataframe["asgPropID"] = asg_prop_id
    # Load the Data in DataFrame into Table
    dataframe.to_sql(name=table, con=conn, schema=schema, if_exists='append', index=False)


def exec_sp_zestimate_temp_in_db(obj_sql: ddsql.DataDeliverySQLServer):
    timeout = 900  # Max number of seconds to wait for procedure to finish execution
    dbconnect = obj_sql.connect_to_sql_db()
    dbcur = dbconnect.cursor()
    # Prepare the stored procedure execution script and parameter values
    stored_proc = "Exec [dbo].[sp_ZEstimate_TempVal]"

    # Execute Stored Procedure With Parameters
    dbcur.execute(stored_proc)
    log.info('Executing [dbo].[sp_ZEstimate_TempVal] stored procedure.')

    # TODO: here's where the magic happens with the nextset() function
    slept = 0
    while dbcur.nextset():
        if slept >= timeout:
            break
        time.sleep(1)
        slept += 1
        # Iterate the cursor
        dbcur.fetchone()

    dbconnect.commit()
    dbcur.close()
    dbconnect.close()
    return True


def generate_zestimate_random_sample(db_obj: ddsql.DataDeliverySQLServer, dc_obj: ddc.DataDeliveryClient):
    dbcur = db_obj.cursor_to_sql_db()

    log.info('Executing SQL Query - Select from IRSPublish..ZEstimate_TempVal')
    dbcur.execute(
        "select t.asgPropID, t.state, t.city, street_name, street_number, zip_code from IRSPublish..ZEstimate_TempVal t where t.asgPropID NOT IN (SELECT r.asgPropID FROM IRSPublish.dbo.ZEstimate_Monthly_RandomSample r)")

    records = dbcur.fetchall()

    log.info('Starting loop')

    for row in records:
        asg_prop_id = row[0]
        state = row[1]
        city = row[2]
        street_name = row[3]
        street_number = row[4]
        zip_code = row[5]
        zbundles = dc_obj.get_zestimates_from_random_sample(state=state, city=city, address=street_name,
                                                            street_number=street_number,
                                                            postal_code=zip_code)

        # create a new Zestimates JSON object
        json_bundles = get_zestimates_json_object(zbundles)
        a_json = json.loads(json_bundles)

        # create a new Zestimates Dataframe
        df = pd.DataFrame.from_dict(a_json, orient="columns")

        # Load the Data in DataFrame into Table
        load_dataframe_into_sqltable(dataframe=df, table=db_obj.table_name, conn=db_obj.conn_str,
                                     schema=db_obj.schema_name, asg_prop_id=asg_prop_id)

    dbcur.close()
    return True


if __name__ == '__main__':
    # get arguments
    args = parse_args()
    zestimates_data_flag = False

    py_file = os.path.splitext(os.path.split(__file__)[1])[0]
    log = AmherstLogger(log_directory=args.log, log_file_app=py_file, vendor_name='Bridge',
                        vendor_product='Zestimates')

    # create a new object of DataDeliveryClient class
    ddc_obj = ddc.DataDeliveryClient(args.api_host, args.access_token)

    # create a new object of DataDeliverySQLServer class
    sql_obj = ddsql.DataDeliverySQLServer(target_server=args.sql_server, target_db=_TARGET_DB,
                                          schema_name=_SCHEMA_NAME, table_name=_TABLE_NAME)

    if exec_sp_zestimate_temp_in_db(obj_sql=sql_obj):
        log.info('Random data for querying Zestimates is Ready!')

        if generate_zestimate_random_sample(db_obj=sql_obj, dc_obj=ddc_obj):
            log.info(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' | Data Imported Successfully')
        else:
            log.info('Something went wrong while generating Zestimate Sample in IRSPublish.dbo.ZEstimate_Monthly_RandomSample table.')

    else:
        log.info('Something went wrong while putting Zestimates Random data into IRSPublish..ZEstimate_TempVal table.')

    log.info('Process finished')
