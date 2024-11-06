#Below code compares data from oracle vs snowflake with list of tables provided in seperate csv file

import modin.pandas as pd
import snowflake.snowpark.modin.plugin
from snowflake.snowpark import Session
import csv
import sys
import requests
import cx_Oracle
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import pd_writer
from snowflake.connector.pandas_tools import write_pandas


CONNECTION_PARAMETERS = {
    'account': '',
    'user': '',
    'authenticator': 'externalbrowser',
    'password': '',
    'role': '',
    'database': '',
    'schema': '',
    'warehouse': '',
}


# Oracle connection details
oracle_username = ''
oracle_password = ''

oracle_host = ''
#oracle_host = ''
oracle_port = '5150'
oracle_service_name = ''
#oracle_service_name = ''
#oracle_table = ''
cx_Oracle.init_oracle_client(lib_dir="C:\\Users\\rbejugam\\Downloads\\instantclient-basic-windows.x64-21.13.0.0.0dbru\\instantclient_21_13")
    

# Function to connect to Oracle database and fetch data
def connect_to_oracle_and_fetch_data(IP_TABLE_NAME,IP_PROD_DATE,IP_FILTER_QUERY):

    ## cx_Oracle.init_oracle_client(lib_dir="/Users/your_username/Downloads/instantclient_19_8")
    connection = cx_Oracle.connect(oracle_username, oracle_password, oracle_host + ':' + oracle_port + '/' + oracle_service_name)
    cursor = connection.cursor()
    ##cursor.execute('SELECT * FROM ' + oracle_table  + 'WHERE PROD_DATE = "2024-04-29"')
    ##cursor.execute('SELECT * FROM LOAD_RLPS.ACCELERATN_DEMAND_ES where ROWNUM <= 10')
    ##cursor.execute("SELECT * FROM LOAD_RLPS.ACCELERATN_DEMAND_ES where PROD_DATE = '5-JUN-2024'")

    #cursor.execute("SELECT PROD_DATE, DELTA_FILE_BYTE, CLIENT_NO, LN_NO, ACCEL_AM, ACCEL_DTE, ACCEL_NULL, ACCEL_RSN_CD,DMD_LTR_EXPIRE_DTE, DMD_LTR_EXPIRE_NULL, AD_DMD_LTR_ISSU_DTE, AD_DMD_LTR_ISSU_NULL,AD_D_LTR_ST_REG_NO, AD_D_ST_REG_DTE, AD_D_ST_REG_NULL, AD_FC_FILNG_ENT_CD, AD_ORG_LNDR_LIC_NO, AD_ORG_LNDR_NO, ACCEL_INT_DUE_AM, LAST_UPDATE_DATE, DEMAND_LTR_DUE_AM, BILLED_CALC_INT_AM, BILLED_INT_CALC_DTE, BILLED_INT_CALC_NULL, INC_ACL_INT_BIL_FG, INC_ACL_INT_CHG_DTE, INC_ACL_INT_CHG_NULL, INC_ACL_INT_CHG_ID, ALLOW_REINST_FG, ALLOW_REINST_DTE, ALLOW_REINST_NULL, ALLOW_REINST_ID FROM LOAD_RLPS.ACCELERATN_DEMAND_ES WHERE PROD_DATE = '05-JUN-2024'")
    #cursor.execute("SELECT CLIENT_NO, LN_NO, ACCEL_DTE, ACCEL_NULL FROM LOAD_RLPS.ACCELERATN_DEMAND_ES  WHERE PROD_DATE = '05-JUN-2024'")
    
    #cursor.execute("SELECT  PROD_DATE , DELTA_FILE_BYTE, CLIENT_NO, LN_NO, ACCEL_AM, ACCEL_DTE, ACCEL_NULL, ACCEL_RSN_CD,DMD_LTR_EXPIRE_DTE, DMD_LTR_EXPIRE_NULL, AD_DMD_LTR_ISSU_DTE, AD_DMD_LTR_ISSU_NULL,AD_D_LTR_ST_REG_NO, AD_D_ST_REG_DTE, AD_D_ST_REG_NULL, AD_FC_FILNG_ENT_CD, AD_ORG_LNDR_LIC_NO, AD_ORG_LNDR_NO, ACCEL_INT_DUE_AM, LAST_UPDATE_DATE, DEMAND_LTR_DUE_AM, BILLED_CALC_INT_AM, BILLED_INT_CALC_DTE, BILLED_INT_CALC_NULL, INC_ACL_INT_BIL_FG, INC_ACL_INT_CHG_DTE, INC_ACL_INT_CHG_NULL, INC_ACL_INT_CHG_ID, ALLOW_REINST_FG, ALLOW_REINST_DTE, ALLOW_REINST_NULL, ALLOW_REINST_ID FROM LOAD_RLPS.ACCELERATN_DEMAND_ES WHERE PROD_DATE = '05-JUN-2024'")
    #query = "SELECT PROD_DATE , DELTA_FILE_BYTE, CLIENT_NO, LN_NO, ACCEL_AM, ACCEL_DTE, ACCEL_NULL, ACCEL_RSN_CD,DMD_LTR_EXPIRE_DTE, DMD_LTR_EXPIRE_NULL, AD_DMD_LTR_ISSU_DTE, AD_DMD_LTR_ISSU_NULL,AD_D_LTR_ST_REG_NO, AD_D_ST_REG_NULL, AD_FC_FILNG_ENT_CD, AD_ORG_LNDR_LIC_NO, AD_ORG_LNDR_NO, ACCEL_INT_DUE_AM, LAST_UPDATE_DATE, DEMAND_LTR_DUE_AM, BILLED_CALC_INT_AM, BILLED_INT_CALC_DTE, BILLED_INT_CALC_NULL, INC_ACL_INT_BIL_FG, INC_ACL_INT_CHG_DTE, INC_ACL_INT_CHG_NULL, INC_ACL_INT_CHG_ID, ALLOW_REINST_FG, ALLOW_REINST_DTE, ALLOW_REINST_NULL, ALLOW_REINST_ID FROM LOAD_RLPS.ACCELERATN_DEMAND_ES WHERE PROD_DATE = '05-JUN-2024'"
    
    #query = "SELECT * FROM LOAD_LPS.LIENHOLDER WHERE PROD_DATE = '05-JUN-2024' " 
    #query = "SELECT * FROM LOAD_LPS." + IP_TABLE_NAME + " WHERE PROD_DATE = '" +IP_PROD_DATE+ "'"
    query = "SELECT * FROM LOAD_LPS." + IP_TABLE_NAME + " WHERE PROD_DATE IN ('" +IP_PROD_DATE+ ")"
    print(query)
    df_ora = pd.read_sql(query, con=connection)
    connection.close()
    return df_ora


def fix_date_cols(df, tz='UTC'):
     cols = df.select_dtypes(include=['datetime64[ns]']).columns
     for col in cols:
         df[col] = df[col].dt.tz_localize(tz)
     return df


session = Session.builder.configs(CONNECTION_PARAMETERS).create()

def compare_tables(IP_TABLE_NAME,PROD_DATE):
    # Snowpark pandas will automatically pick up the Snowpark session created above.
    # It will use that session to create new dataframes.
    #oracle_df = connect_to_oracle_and_fetch_data()
    #df_2 = fix_date_cols(oracle_df)
    #SELECT * EXCLUDE (PROD_DATE) FROM TEST_WKS_SERVICING_ANALYTICS.LOAD_RLPS_PA.ORACLE_ACCELERATN_DEMAND
    #MINUS
    #IP_TABLE_NAME = 'ACCELERATN_DEMAND'
    #PROD_DATE = "2024-06-05"
    query = """
    SELECT * EXCLUDE (PROD_DATE,BATCH_PROC_DATE,CREATE_TS,CREATE_USER_ID,UPDATE_TS,UPDATE_USER_ID)  
    FROM TEST_WKS_SERVICING_ANALYTICS.DMS_BKFIL_LOAD_LPS.SNOW_PROD_LPS_"""+IP_TABLE_NAME+""" 
    --WHERE PROD_DATE ='""" +PROD_DATE+"""'
    WHERE PROD_DATE in ( '""" +PROD_DATE+""")
    MINUS
    SELECT * EXCLUDE (PROD_DATE) FROM TEST_WKS_SERVICING_ANALYTICS.DMS_BKFIL_LOAD_LPS.ORACLE_"""+IP_TABLE_NAME+"""
   --WHERE PROD_DATE in ( '""" +PROD_DATE+""")
    """
    print(query)
    #df = session.sql("SELECT * EXCLUDE (PROD_DATE) FROM TEST_WKS_SERVICING_ANALYTICS.LOAD_LPS.ESCROW_MGMT limit 10")
    df = session.sql(query)
    # Create pandas DataFrame
    #pandas_df = pd.DataFrame(df)
    pandas_df = df.to_pandas()
    #pandas_df.to_csv('C:\etl_design\comapre_snow_ora.csv', header=False, index=False, float_format='%.4f')
    #pandas_df.show()
    # Display records without column names
    #print(pandas_df.to_string())
    # Get the count of records
    record_count = len(pandas_df)
    # Open a file named 'output.txt' in write mode
    with open('C:\etl_design\\recon_output.txt', 'a') as f:
    # Write into the file
        if(record_count > 0):
            with open('C:\etl_design\\mismatch_output.txt', 'a') as f22:
                f22.write("\nThis Table is Not Matching with Oracle:" + IP_TABLE_NAME + " And Mismatch Record Count is: " + str(record_count) +"")
                exit
        else:
            f.write("\nThis Table is Exactly Matching with Oracle:" + IP_TABLE_NAME + " And Match Record Count is: " + str(record_count) +"")

# Open the CSV file
with open('C:\etl_design\\test_dms_rlps_master_ora.csv', newline='') as csvfile:
    # Create a CSV reader object
    reader = csv.reader(csvfile)
    # Iterate over each row in the CSV file
    # Skip the first row
    next(reader)
    
    for row in reader:
        # Assuming there are exactly two columns in each row
        if len(row) == 3:
            # Pass the values to the function for printing
            #print_values(row[0], row[1])
            #print(row[0])
            IP_TABLE_NAME = row[0]
            IP_PROD_DATE = row[1]
            IP_FILTER_QUERY = row[2]
            #print(IP_TABLE_NAME)
            TGT_TBL_NM = "ORACLE_"+row[0]
            print(TGT_TBL_NM)
            IP_TABLE_NAME = row[0]
            try:
                 #oracle_df = connect_to_oracle_and_fetch_data(IP_TABLE_NAME,IP_PROD_DATE,IP_FILTER_QUERY)
                 #df_2 = fix_date_cols(oracle_df)
                 #snowpark_df = session.write_pandas(oracle_df, TGT_TBL_NM, auto_create_table=True, overwrite=True,use_logical_type=True)
                 compare_tables(IP_TABLE_NAME,IP_PROD_DATE)

            except Exception as e:
                print(f"Unhandled error occurred: {e}")
                with open('C:\etl_design\\load_log.txt', 'a') as f:
                        # Write into the file
                        f.write("\n Error Loading Oracle Table :" + IP_TABLE_NAME + "")

            #snowpark_df = session.write_pandas(oracle_df, TGT_TBL_NM,overwrite=False,use_logical_type=True)
            #compare_tables(IP_TABLE_NAME,IP_PROD_DATE)

            


#print(query)
#snowpark_df = session.write_pandas(oracle_df, "ORACLE_LIENHOLDER", auto_create_table=True, use_logical_type=True)
#snowpark_df = session.write_pandas(df_2, "ORACLE_ACCELERATN_DEMAND_ES")
#df2 = pd.read_snowflake('ORACLE_ACCELERATN_DEMAND_ES')
