
###The below pyspark script contains the following functions read_from_source,transformation_template,dq_check,data_profile,write_to_target,email_report
###read_from_source() function will read from given source and write to intermediate cloud storage in temp location as parquet format each run will create new run id for read_from_source step and update the metadata if job fail or success
###transformation_step() function will read from temp cloud storage location  and apply transformation and store temp cloud storage location based on run_id and update the metadata if job fail or success
###dq_check() function will read from temp cloud storagelocation and apply data quality check and store temp cloud storage location  based on run_id and update the metadata if job fail or success
###data_profiling() function will read from temp cloud storage location  and apply data profiling check and store temp cloud storage location based on run_id and update the metadata if job fail or success
###write_to_target() function will read from temp cloud storage location  and write to given target and update the metadata if job fail or success
###email_step() function will send a mail also delete the file from temp cloud storage location 
###The logs will generate in cloud storage workflow_logs/workflow_name folder with step name

workflow_name='testing_testing'
workflow_id=20001741
local_timezone='Asia/Calcutta'


#Dependency package  
import io
import pymysql
from pyspark.sql.functions import *
from datetime import datetime, timedelta
from pytz import timezone 
from pyspark.sql import SparkSession
from pyspark import SparkContext
import hashlib
from pyspark.sql.types import *
import logging
import os
import time
import json
import importlib.machinery
import sys
import paramiko
import pandas as pd
import gzip
import shutil
from salesforce_bulk.util import IteratorBytesIO
from salesforce_bulk import SalesforceBulk
from azure.storage.blob import BlobClient,ContainerClient
from support_files.dq_module.validate_df import ValidateSparkDataFrame
from support_files.config import constants as cs
from support_files.data_mapping import data_mapping
from support_files.email_templates import mailer as mailer_module
from support_files.credentials.password_details import decrypt_details


#rds details
host = cs.metadata_host
database = cs.metadata_database
port = cs.metadata_port

#metadata user name
user_name = cs.metadata_user_name
password = cs.metadata_passwd


logging.Formatter.converter = lambda *args: datetime.now(timezone(local_timezone)).timetuple()

formatter = logging.Formatter('[%(asctime)s] %(filename)s:%(lineno)d} %(levelname)s - %(message)s')
    
transformation_flag='N'
dq_checks_flag='N'
data_profile_flag='N'
dq_table_check_flag='N'

# For Capturing Run Status Details
run_details_dict = {}
    

def read_from_source():

    start_time=datetime.now()
    value_type='READ_FROM_SOURCE'
    # connect to metadata
    conn = pymysql.connect(host = host,port=port,user = user_name,password = password,db = database, charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()  

    # fetch current id
    cursor.execute("""select max(run_id)+1 as current_run_id from workflow_execution_details
                        where workflow_id_id='{}'""".format(workflow_id))
    current_run_id=cursor.fetchone()
    if current_run_id['current_run_id']==None:
        current_run_id=1000
    else:
        current_run_id=current_run_id['current_run_id']

    
    # write log to azure container
    output = io.StringIO()
    handler = logging.StreamHandler(output)
    handler.setFormatter(formatter)
    logger = logging.getLogger('root')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
        

    # fetch workflow details
    query="""select workflow_id,source_details,source_load_type,source_load_details,last_run_details from workflow_details
                where workflow_id='{}'""".format(workflow_id)
    cursor.execute(query)
    conn.commit()
    record = cursor.fetchone()
    logger.info('metadata query :'+query)
    logger.info(record)
    source_details=record['source_details']
    source_details=json.loads(source_details)

    # fetch connection details
    cursor.execute("""select connection_details,password from connection_details
                    where connection_id='{}'""".format(source_details['connection_id']))
    source_connection_details=cursor.fetchone()
    conn.commit()
    source_connection=source_connection_details['connection_details']
    source_connection=json.loads(source_connection)

    query = """UPDATE workflow_details SET last_run_status='Running',active_indicator='Y',last_run_date='{}',last_run_id={} WHERE workflow_id='{}'""".format(start_time,current_run_id,workflow_id)
    cursor.execute(query)
    conn.commit()

    # fetch user details
    logger.info("Initiating Reading from Source")
    try:
        #read from source
        target_col_data_type = data_mapping.get_target_col_data_type(workflow_id)
        target_col = data_mapping.get_target_columns(workflow_id)
        
        SparkContext.setSystemProperty('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        SparkContext.setSystemProperty('spline.mode',cs.SPLINE_MODE)
        SparkContext.setSystemProperty("spark.sql.queryExecutionListeners", "za.co.absa.spline.harvester.listener.SplineQueryExecutionListener")
        SparkContext.setSystemProperty('spline.producer.url', cs.SPLINE_PRODUCER_URL)
        SparkContext._jvm.za.co.absa.spline.harvester.SparkLineageInitializer.enableLineageTracking(spark._jsparkSession)
        spark.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs",cs.Legacy_Set_Command_Rejects)
        spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation",cs.Legacy_Allow_Creating_Managed_Table)
        spark.conf.set("spark.sql.legacy.timeParserPolicy",cs.Legacy_Time_Parser_Policy)
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")



        #get new file from blob
        latest_datetime_list=[]
        blob_file_list=[]
        container_name=source_details['Container_Name']
        account_name=source_details['Storage_Account_Name']
        sas_token=decrypt_details.get_password(source_connection_details['password'])
        def credentials():
            container = ContainerClient.from_container_url(
            container_url=f"https://{account_name}.blob.core.windows.net/{container_name}?",
            credential=sas_token)
            return container
        def get_filelocation(file_location):
            logger.info(file_location)
            if '*' in file_location:
                txt=file_location.split('*')
            else:
                if '/' not in file_location:
                    txt = ['', file_location]
                else:
                    txt = file_location.rsplit("/", 1)
            
            container = credentials()
            
            if txt[1] == '':
                files = [{'key': blob.name, 'last_modified': blob.last_modified} for blob in container.list_blobs(name_starts_with=txt[0])]
            else:
                if '*' in file_location:
                    txt=file_location.split('*')
                    if txt[1]=='':
                        files = [{'key': blob.name, 'last_modified': blob.last_modified} for blob in container.list_blobs(name_starts_with=txt[0])
                            if blob.name.startswith(txt[0])]
                    else:
                        files = [{'key': blob.name, 'last_modified': blob.last_modified} for blob in container.list_blobs(name_starts_with=txt[0])
                            if blob.name.startswith(txt[0]) and blob.name.endswith(txt[1])]
                
                else:
                    files = [{'key': blob.name, 'last_modified': blob.last_modified} for blob in container.list_blobs(name_starts_with=txt[0])
                            if blob.name.startswith(file_location)]

            my_list = sorted(files, key=lambda k: k['last_modified'], reverse=True)
            if 'is_collation' in source_details:
                logger.info('Getting the file path for collation')
                if source_details['is_collation']==True:
                    if record['last_run_details'] == None or record['last_run_details'] == '':
                        file_details = {}
                    else:
                        file_details = json.loads(record['last_run_details'])
                    for i in my_list:
                        if 'last_load_file_detail' in file_details:
                            last_load_file_datetime = file_details['last_load_file_detail']
                            last_load_file_datetime = datetime.fromisoformat(last_load_file_datetime)
                            utc_datetime = i['last_modified']
                            timezone_local = timezone(local_timezone)
                            local_datetime = utc_datetime.astimezone(timezone_local)
                            if local_datetime > last_load_file_datetime:
                                latest_datetime_list.append(i['last_modified'])
                                blob_file_path = "wasbs://{}@{}.blob.core.windows.net/{}".format(container_name,account_name, i['key'])
                                blob_file_list.append(blob_file_path)
                        else:
                            latest_datetime_list.append(i['last_modified'])
                            blob_file_path = "wasbs://{}@{}.blob.core.windows.net/{}".format(container_name,account_name, i['key'])
                            blob_file_list.append(blob_file_path)

                    if 'last_load_file_detail' in file_details:
                        last_load_file_datetime = file_details['last_load_file_detail']
                        last_load_file_datetime = datetime.fromisoformat(last_load_file_datetime)
                        if latest_datetime_list==[]:
                            raise ValueError('Load from source failed new file is not arrived')
                        for i in latest_datetime_list:
                            utc_datetime = i
                            timezone_local = timezone(local_timezone)
                            local_datetime = utc_datetime.astimezone(timezone_local)
                            time_diff = local_datetime - last_load_file_datetime
                            total_time_diff = int(time_diff.total_seconds())
                            print(total_time_diff)
                            if total_time_diff <= 0:
                                raise ValueError('Load from source failed new file is not arrived')
                        return blob_file_list
                    else:
                        return blob_file_list

            else:
                logger.info('Getting the file path')
                blob_file_path = "wasbs://{}@{}.blob.core.windows.net/{}".format(container_name,account_name, my_list[0]['key'])
                blob_file_list.append(blob_file_path)
                utc_datetime = my_list[0]['last_modified']
                timezone_local = timezone(local_timezone)
                local_datetime=utc_datetime.astimezone(timezone_local)
                logger.info(local_datetime)
                if record['last_run_details'] == None or record['last_run_details'] == '':
                    file_details = {}
                else:
                    file_details = json.loads(record['last_run_details'])
                if 'last_load_file_detail' in file_details:
                    last_load_file_datetime = file_details['last_load_file_detail']
                    last_load_file_datetime = datetime.fromisoformat(last_load_file_datetime)
                    time_diff = local_datetime - last_load_file_datetime
                    total_time_diff = int(time_diff.total_seconds())
                    logger.info(total_time_diff)
                    if total_time_diff <= 0:
                        raise ValueError('Load from source failed new file is not arrived')
                    else:
                        latest_datetime_list.append(local_datetime)
                        return blob_file_list
                else:
                    latest_datetime_list.append(local_datetime)
                    return blob_file_list

        file_location = source_details['file_location'].split('/')
        file_loaction_blob=''
        for i in range(1,len(file_location)-1):
            file_loaction_blob+=file_location[i]+'/'
        file_loaction_blob+=file_location[len(file_location)-1]
        path=get_filelocation(file_loaction_blob)
        logger.info("source file path : {}".format(path))
        logger.info('Number of files reading from source : {}'.format(len(path)))
        spark.conf.set(f"fs.azure.sas.{container_name}.{account_name}.blob.core.windows.net",sas_token)

        df = (spark.read
              .format("csv")
              .option("header",source_details['first_row_is_header'])
              .option("delimiter","|")
              .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SZ")
              .schema(target_col_data_type)
              .load(path))
        if record['last_run_details'] == None or record['last_run_details'] == '':
            file_details = {}
        else:
            file_details = json.loads(record['last_run_details'])
        latest_datetime_list = sorted(latest_datetime_list, reverse=True)
        file_details['last_load_file_detail'] = str(latest_datetime_list[0])
        file_details=json.dumps(file_details)
        query = f"""UPDATE workflow_details SET last_run_details='{file_details}' where workflow_id={workflow_id}"""
        cursor.execute(query)
        conn.commit()
    

        target_col_list=data_mapping.get_selected_columns_list(workflow_id)
        df=df.select(target_col_list)
        number_of_rows = df.count()
        logger.info('schema {}'.format(target_col_data_type))
        logger.info("number of rows extracted from source {}".format(number_of_rows))

        # Run Status Capturing
        source_path_details = {"source_file_path":path}
        source_files_details = {"source_file_count":len(path)}
        source_row_count_details = {"source_row_count":number_of_rows}
        source_column_count_details = {"source_column_count":len(df.columns)}
        latest_datetime_list = sorted(latest_datetime_list, reverse=True)
        source_file_arrival = {"source_file_arrival": str(latest_datetime_list[0])}
        run_details_dict.update({**source_path_details, **source_files_details, **source_row_count_details, **source_column_count_details, **source_file_arrival})
        run_details = json.dumps(run_details_dict)
        
        if number_of_rows==0:
            raise ValueError('Source as empty records')
        check_source=cursor.execute("""select * from workflow_execution_details where step_name='Read_from_source' and run_id={} and workflow_id_id={}""".format(current_run_id,workflow_id))
        logger.info("updating metadata")
        if check_source==1:
            query="""UPDATE workflow_execution_details SET step_completion_status ='success',step_started='{}',step_completion='{}',run_details='{}' where step_name='Read_from_source' and run_id={} and workflow_id_id={}""".format(start_time,datetime.now(),run_details,current_run_id,workflow_id)
            cursor.execute(query)
            conn.commit()
        else:
            query="""INSERT INTO workflow_execution_details (step_name,step_completion_status,workflow_id_id,step_started,step_completion,run_id,run_details)
            VALUES ('Read_from_source','success', {},'{}','{}',{},'{}')""".format(workflow_id,start_time,datetime.now(),current_run_id,run_details)
            cursor.execute(query)
            conn.commit()
        file_name = "{}_{}".format(workflow_name, current_run_id)
        
        spark.conf.set(f"fs.azure.sas.{cs.AZURE_TEMP_BLOB_CONTAINER}.{cs.AZURE_TEMP_BLOB_ACCOUNT}.blob.core.windows.net",cs.AZURE_TEMP_BLOB_TOKEN)
        
        df.write.mode("overwrite").parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, file_name))
        logger.info("Data has been successfully read from source")
        
        container = ContainerClient.from_container_url(
                container_url=f"https://{cs.AZURE_EXTRACTOR_CONFIG_ACCOUNT}.blob.core.windows.net/{cs.AZURE_EXTRACTOR_CONFIG_CONTAINER}?",
                credential=cs.AZURE_EXTRACTOR_CONFIG_SAS_TOKEN)
        log_file=container.get_blob_client("workflow_log/{}_{}/{}/{}_{}".format(workflow_name, workflow_id, current_run_id,value_type,datetime.now()))
        log_file.upload_blob(output.getvalue(),overwrite=True)
        
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(e)
        logger.info("Load from source Failed")
        logger.info("updating metadata")

        # ERROR LOG
        error_details = {"error_log": f'{e}'}
        run_details_dict.update(error_details)
        run_details = json.dumps(run_details_dict)
        
        query = """UPDATE workflow_details SET last_run_status='Failed' , active_indicator='N',last_run_id={} WHERE workflow_id='{}'""".format(current_run_id,workflow_id)
        cursor.execute(query)
        conn.commit()
        check_source=cursor.execute("""select * from workflow_execution_details where step_name='Read_from_source' and run_id={} and workflow_id_id={}""".format(current_run_id,workflow_id))
        if check_source==1:
            query="""UPDATE workflow_execution_details SET step_completion_status ='Failed',step_started='{}',step_completion='{}',run_details='{}' where step_name='Read_from_source' and run_id={} and workflow_id_id={}""".format(start_time,datetime.now(),run_details,current_run_id,workflow_id)
            b=cursor.execute(query)
            conn.commit()
        else:
            query="""INSERT INTO workflow_execution_details (step_name,step_completion_status,workflow_id_id,step_started,step_completion,run_id,run_details)
            VALUES ('Read_from_source','Failed', {},'{}','{}',{},'{}')""".format(workflow_id,start_time,datetime.now(),current_run_id,run_details)
            b=cursor.execute(query)
            conn.commit()
        
        container = ContainerClient.from_container_url(
                container_url=f"https://{cs.AZURE_EXTRACTOR_CONFIG_ACCOUNT}.blob.core.windows.net/{cs.AZURE_EXTRACTOR_CONFIG_CONTAINER}?",
                credential=cs.AZURE_EXTRACTOR_CONFIG_SAS_TOKEN)
        log_file=container.get_blob_client("workflow_log/{}_{}/{}/{}_{}".format(workflow_name, workflow_id, current_run_id,value_type,datetime.now()))
        log_file.upload_blob(output.getvalue(),overwrite=True)
        
        cursor.close()
        conn.close()
        raise ValueError('Load from source failed')
    logging.shutdown()
read_from_source()
    

##################################################


def transformation_step():   
    start_time=datetime.now()
    value_type='Transformation'
    # connect to metadata
    conn = pymysql.connect(host = host,port=port,user = user_name,password = password,db = database, charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()  

    # fetch current id
    cursor.execute("""select max(run_id) as current_run_id from workflow_execution_details
                        where workflow_id_id='{}'""".format(workflow_id))
    current_run_id=cursor.fetchone()
    current_run_id=current_run_id['current_run_id']
    
    
    # write log to azure container
    output = io.StringIO()
    handler = logging.StreamHandler(output)
    handler.setFormatter(formatter)
    logger = logging.getLogger('root')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
        
       
    try:
        
        SparkContext.setSystemProperty('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        SparkContext.setSystemProperty('spline.mode',cs.SPLINE_MODE)
        SparkContext.setSystemProperty("spark.sql.queryExecutionListeners", "za.co.absa.spline.harvester.listener.SplineQueryExecutionListener")
        SparkContext.setSystemProperty('spline.producer.url', cs.SPLINE_PRODUCER_URL)
        SparkContext._jvm.za.co.absa.spline.harvester.SparkLineageInitializer.enableLineageTracking(spark._jsparkSession)
        spark.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs",cs.Legacy_Set_Command_Rejects)
        spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation",cs.Legacy_Allow_Creating_Managed_Table)
        spark.conf.set("spark.sql.legacy.timeParserPolicy",cs.Legacy_Time_Parser_Policy)
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

        
        spark.conf.set(f"fs.azure.sas.{cs.AZURE_TEMP_BLOB_CONTAINER}.{cs.AZURE_TEMP_BLOB_ACCOUNT}.blob.core.windows.net",cs.AZURE_TEMP_BLOB_TOKEN)
        
        file_name = "{}_{}".format(workflow_name, current_run_id)
        df=spark.read.parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, file_name))
         
        file_name = "intermediate_{}_{}".format(workflow_name, current_run_id)

        # Get Run Status
        run_details_query = """select max(run_details) as run_details from workflow_execution_details where workflow_id_id='{}' and run_id='{}'""".format(workflow_id,current_run_id)
        cursor.execute(run_details_query)
        run_details_record=cursor.fetchone()
        source_run_details=json.loads(run_details_record['run_details'])
        # Update Run Status
        transformation_column_details = {"transformation_column_count":len(df.columns)}
        run_details_dict.update({**source_run_details,**transformation_column_details})
        run_details = json.dumps(run_details_dict)
        
        df.write.mode("overwrite").parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, file_name))
       
        check_source=cursor.execute("""select * from workflow_execution_details where step_name='Transformation_step' and run_id={} and workflow_id_id={}""".format(current_run_id,workflow_id))
        if check_source==1:
            query="""UPDATE workflow_execution_details SET step_completion_status ='success',run_details='{}' where step_name='Transformation_step' and run_id={} and workflow_id_id={}""".format(run_details,current_run_id,workflow_id)
            b=cursor.execute(query)
            conn.commit()
        else:
            query="""INSERT INTO workflow_execution_details (step_name,step_completion_status,workflow_id_id,step_started,step_completion,run_id,run_details)
            VALUES ('Transformation_step','success', {},'{}','{}',{},'{}')""".format(workflow_id,start_time,datetime.now(),current_run_id,run_details)
            b=cursor.execute(query)
            conn.commit()
        logger.info("Applying Transformation success")
        
        container = ContainerClient.from_container_url(
                container_url=f"https://{cs.AZURE_EXTRACTOR_CONFIG_ACCOUNT}.blob.core.windows.net/{cs.AZURE_EXTRACTOR_CONFIG_CONTAINER}?",
                credential=cs.AZURE_EXTRACTOR_CONFIG_SAS_TOKEN)
        log_file=container.get_blob_client("workflow_log/{}_{}/{}/{}_{}".format(workflow_name, workflow_id, current_run_id,value_type,datetime.now()))
        log_file.upload_blob(output.getvalue(),overwrite=True)
        
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(e)
        logger.info("Applying Transformation Failed")
        
        # GET 
        run_details_query = """select max(run_details) as run_details from workflow_execution_details where workflow_id_id='{}' and run_id='{}'""".format(workflow_id,current_run_id)
        cursor.execute(run_details_query)
        run_details_record=cursor.fetchone()
        source_run_details=json.loads(run_details_record['run_details'])

        # ERROR LOG
        error_details = {"error_log": f"{e}"}
        run_details_dict.update({**source_run_details,**error_details})
        run_details = json.dumps(run_details_dict)

        query = """UPDATE workflow_details SET last_run_status='Failed' , active_indicator='N',last_run_id={} WHERE workflow_name='{}'""".format(current_run_id,workflow_name)
        cursor.execute(query)
        conn.commit()
        check_source=cursor.execute("""select * from workflow_execution_details where step_name='Transformation_step' and run_id={} and workflow_id_id={}""".format(current_run_id,workflow_id))
        if check_source==1:
            query="""UPDATE workflow_execution_details SET step_completion_status ='Failed',step_started='{}',step_completion='{}',run_details='{}' where step_name='Transformation_step' and run_id={} and workflow_id_id={}""".format(start_time,datetime.now(),run_details,current_run_id,workflow_id)
            b=cursor.execute(query)
            conn.commit()
        else:
            query="""INSERT INTO workflow_execution_details (step_name,step_completion_status,workflow_id_id,step_started,step_completion,run_id,run_details)
            VALUES ('Transformation_step','Failed', {},'{}','{}',{},'{}')""".format(workflow_id,start_time,datetime.now(),current_run_id,run_details)
            b=cursor.execute(query)
            conn.commit()
        
        container = ContainerClient.from_container_url(
                container_url=f"https://{cs.AZURE_EXTRACTOR_CONFIG_ACCOUNT}.blob.core.windows.net/{cs.AZURE_EXTRACTOR_CONFIG_CONTAINER}?",
                credential=cs.AZURE_EXTRACTOR_CONFIG_SAS_TOKEN)
        log_file=container.get_blob_client("workflow_log/{}_{}/{}/{}_{}".format(workflow_name, workflow_id, current_run_id,value_type,datetime.now()))
        log_file.upload_blob(output.getvalue(),overwrite=True)
        
        cursor.close()
        conn.close()
        raise ValueError('Applying Transformation Failed')

    logging.shutdown()
transformation_step()
    


def dq_check():
    start_time=datetime.now()
    value_type='DQ_check'
    # connect to metadata
    conn = pymysql.connect(host = host,port=port,user = user_name,password = password,db = database, charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor() 

    # fetch current id
    cursor.execute("""select max(run_id) as current_run_id from workflow_execution_details
                        where workflow_id_id='{}'""".format(workflow_id))
    current_run_id=cursor.fetchone()
    current_run_id=current_run_id['current_run_id']

    
    # write log to azure container
    output = io.StringIO()
    handler = logging.StreamHandler(output)
    handler.setFormatter(formatter)
    logger = logging.getLogger('root')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
        

    # fetch workflow details
    query="""select workflow_id,source_details,source_load_type,source_load_details,target_load_details from workflow_details
                where workflow_id='{}'""".format(workflow_id)
    cursor.execute(query)
    conn.commit()
    record = cursor.fetchone()
    logger.info('metadata query :'+query)
    logger.info(record)
    source_details=record['source_details']
    source_details=json.loads(source_details)
    target_table_name=record['target_load_details']
    target_table_name=json.loads(target_table_name)
    target_table_name=str.upper(target_table_name['current_table'])

    # fetch connection details
    cursor.execute("""select connection_details,password from connection_details
                    where connection_id='{}'""".format(source_details['connection_id']))
    source_connection_details=cursor.fetchone()
    conn.commit()
    source_connection=source_connection_details['connection_details']
    source_connection=json.loads(source_connection)

    logger.info('Initiating DQ Check')   

    try:
        
        SparkContext.setSystemProperty('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        SparkContext.setSystemProperty('spline.mode',cs.SPLINE_MODE)
        SparkContext.setSystemProperty("spark.sql.queryExecutionListeners", "za.co.absa.spline.harvester.listener.SplineQueryExecutionListener")
        SparkContext.setSystemProperty('spline.producer.url', cs.SPLINE_PRODUCER_URL)
        SparkContext._jvm.za.co.absa.spline.harvester.SparkLineageInitializer.enableLineageTracking(spark._jsparkSession)
        spark.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs",cs.Legacy_Set_Command_Rejects)
        spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation",cs.Legacy_Allow_Creating_Managed_Table)
        spark.conf.set("spark.sql.legacy.timeParserPolicy",cs.Legacy_Time_Parser_Policy)
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

        
        spark.conf.set(f"fs.azure.sas.{cs.AZURE_TEMP_BLOB_CONTAINER}.{cs.AZURE_TEMP_BLOB_ACCOUNT}.blob.core.windows.net",cs.AZURE_TEMP_BLOB_TOKEN)
        
        if dq_checks_flag=='Y':       
            if transformation_flag=='Y':			
                file_name = "intermediate_{}_{}".format(workflow_name, current_run_id)
                df = spark.read.parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, file_name))
            else:
                file_name = "{}_{}".format(workflow_name, current_run_id)
                df = spark.read.parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, file_name))

            result = ValidateSparkDataFrame(spark, df).execute()

            file_name_erroneous = "dq_erroneous_{}_{}".format(workflow_name, current_run_id)

            file_name_correct = "dq_correct_{}_{}".format(workflow_name, current_run_id)

            file_name_errors = "dq_validation_{}_{}".format(workflow_name, current_run_id)

            # GET RUN STATUS 
            run_details_query = """select max(run_details) as run_details from workflow_execution_details where workflow_id_id='{}' and run_id='{}'""".format(workflow_id,current_run_id)
            cursor.execute(run_details_query)
            run_details_record=cursor.fetchone()
            transformation_run_details=json.loads(run_details_record['run_details'])

            # Dq runstatus
            dq_row_details = {'dq_row_count':result.correct_data.count()}
            run_details_dict.update({**transformation_run_details,**dq_row_details})
            run_details =json.dumps(run_details_dict)

            result.correct_data.write.mode("overwrite").parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, file_name_correct))

            result.erroneous_data.write.mode("overwrite").parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, file_name_erroneous))

            if len(result.errors) > 0:
                validation_summary = spark.createDataFrame(result.errors)
            else:
                schema = StructType([StructField("Strategy",StringType(),True), StructField("Check",StringType(),True), StructField("Column",StringType(),True), StructField("Status", StringType(), True), StructField("Impacted_Records", IntegerType(), True), StructField("Action_Taken", StringType(), True)])
                validation_summary = spark.createDataFrame(result.errors, schema=schema)

            validation_summary.write.mode("overwrite").parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, file_name_errors))        

        if dq_table_check_flag=='Y':
            query="""select dq_table_check,dq_table_result from dq_table_details where workflow_id={}""".format(workflow_id)
            cursor.execute(query)
            dq_table_details=cursor.fetchone()
            count_details=dq_table_details['dq_table_result']
            count_details=json.loads(count_details)
            
        check_source=cursor.execute("""select * from workflow_execution_details where step_name='DQ_Step' and run_id={} and workflow_id_id={}""".format(current_run_id,workflow_id))
        if check_source==1:
            query="""UPDATE workflow_execution_details SET step_completion_status ='success',run_details='{}' where step_name='DQ_Step' and run_id={} and workflow_id_id={}""".format(run_details,current_run_id,workflow_id)
            b=cursor.execute(query)
            conn.commit()
        else:
            query="""INSERT INTO workflow_execution_details (step_name,step_completion_status,workflow_id_id,step_started,step_completion,run_id,run_details)
            VALUES ('DQ_Step','success', {},'{}','{}',{},'{}')""".format(workflow_id,start_time,datetime.now(),current_run_id,run_details)
            b=cursor.execute(query)
            conn.commit()
        logger.info("Applying DQ Check success")
        
        container = ContainerClient.from_container_url(
                container_url=f"https://{cs.AZURE_EXTRACTOR_CONFIG_ACCOUNT}.blob.core.windows.net/{cs.AZURE_EXTRACTOR_CONFIG_CONTAINER}?",
                credential=cs.AZURE_EXTRACTOR_CONFIG_SAS_TOKEN)
        log_file=container.get_blob_client("workflow_log/{}_{}/{}/{}_{}".format(workflow_name, workflow_id, current_run_id,value_type,datetime.now()))
        log_file.upload_blob(output.getvalue(),overwrite=True)
        
        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(e)
        logger.info("Applying DQ Check Failed")
        
        # GET RUN STATUS 
        run_details_query = """select max(run_details) as run_details from workflow_execution_details where workflow_id_id='{}' and run_id='{}'""".format(workflow_id,current_run_id)
        cursor.execute(run_details_query)
        run_details_record=cursor.fetchone()
        transformation_run_details=json.loads(run_details_record['run_details'])

        # ERROR LOG
        error_details = {"error_log":f"{e}"}
        run_details_dict.update({**transformation_run_details,**error_details})
        run_details = json.dumps(run_details_dict)
        
        query = """UPDATE workflow_details SET last_run_status='Failed' , active_indicator='N',last_run_id={} WHERE workflow_name='{}'""".format(current_run_id,workflow_name)
        cursor.execute(query)
        conn.commit()
        check_source=cursor.execute("""select * from workflow_execution_details where step_name='DQ_Step' and run_id={} and workflow_id_id={}""".format(current_run_id, workflow_id))
        if check_source==1:
            query="""UPDATE workflow_execution_details SET step_completion_status ='Failed',step_started='{}',step_completion='{}',run_details='{}' where step_name='DQ_Step' and run_id={} and workflow_id_id={}""".format(start_time,datetime.today(),run_details,current_run_id, workflow_id)
            b=cursor.execute(query)
            conn.commit()
        else:
            query="""INSERT INTO workflow_execution_details (step_name,step_completion_status,workflow_id_id,step_started,step_completion,run_id,run_details)
            VALUES ('DQ_Step','Failed', {},'{}','{}',{},'{}')""".format(workflow_id,start_time,datetime.now(),current_run_id,run_details)
            b=cursor.execute(query)
            conn.commit()
        
        container = ContainerClient.from_container_url(
                container_url=f"https://{cs.AZURE_EXTRACTOR_CONFIG_ACCOUNT}.blob.core.windows.net/{cs.AZURE_EXTRACTOR_CONFIG_CONTAINER}?",
                credential=cs.AZURE_EXTRACTOR_CONFIG_SAS_TOKEN)
        log_file=container.get_blob_client("workflow_log/{}_{}/{}/{}_{}".format(workflow_name, workflow_id, current_run_id,value_type,datetime.now()))
        log_file.upload_blob(output.getvalue(),overwrite=True)
        
        cursor.close()
        conn.close()
        raise ValueError('Applying DQ Check Failed')
    logging.shutdown()
dq_check()
    


def data_profiling():
    start_time=datetime.now()
    value_type='data_profile'
    # connect to metadata
    conn = pymysql.connect(host = host,port=port,user = user_name,password = password,db = database, charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()  

    # fetch current id
    cursor.execute("""select max(run_id) as current_run_id from workflow_execution_details
                        where workflow_id_id='{}'""".format(workflow_id))
    current_run_id=cursor.fetchone()
    current_run_id=current_run_id['current_run_id']
    
    
    # write log to azure container
    output = io.StringIO()
    handler = logging.StreamHandler(output)
    handler.setFormatter(formatter)
    logger = logging.getLogger('root')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
        
    
    # fetch workflow details
    query="""select workflow_id from workflow_details
                where workflow_name='{}'""".format(workflow_name)
    cursor.execute(query)
    conn.commit()
    record = cursor.fetchone()
    logger.info('Initiating Data Profiling')

    try:
        
        SparkContext.setSystemProperty('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        SparkContext.setSystemProperty('spline.mode',cs.SPLINE_MODE)
        SparkContext.setSystemProperty("spark.sql.queryExecutionListeners", "za.co.absa.spline.harvester.listener.SplineQueryExecutionListener")
        SparkContext.setSystemProperty('spline.producer.url', cs.SPLINE_PRODUCER_URL)
        SparkContext._jvm.za.co.absa.spline.harvester.SparkLineageInitializer.enableLineageTracking(spark._jsparkSession)
        spark.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs",cs.Legacy_Set_Command_Rejects)
        spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation",cs.Legacy_Allow_Creating_Managed_Table)
        spark.conf.set("spark.sql.legacy.timeParserPolicy",cs.Legacy_Time_Parser_Policy)
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

        
        spark.conf.set(f"fs.azure.sas.{cs.AZURE_TEMP_BLOB_CONTAINER}.{cs.AZURE_TEMP_BLOB_ACCOUNT}.blob.core.windows.net",cs.AZURE_TEMP_BLOB_TOKEN)
              
        if transformation_flag=='T':
            file_name = "intermediate_{}_{}".format(workflow_name, current_run_id)
            df = spark.read.parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, file_name))
        else:
            file_name = "{}_{}".format(workflow_name, current_run_id)
            df = spark.read.parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, file_name))

        result = ValidateSparkDataFrame(spark, df).execute_dataprofile()

        file_name = "data_profile_{}_{}".format(workflow_name, current_run_id)
        result.write.mode("overwrite").parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, file_name))

        check_source=cursor.execute("""select * from workflow_execution_details where step_name='data_profile_Step' and run_id={} and workflow_id_id={}""".format(current_run_id, workflow_id))
        if check_source==1:
            query="""UPDATE workflow_execution_details SET step_completion_status ='success' where step_name='data_profile_Step' and run_id={} and workflow_id_id={}""".format(current_run_id,workflow_id)
            b=cursor.execute(query)
            conn.commit()
        else:
            query="""INSERT INTO workflow_execution_details (step_name,step_completion_status,workflow_id_id,step_started,step_completion,run_id)
            VALUES ('data_profile_Step','success', {},'{}','{}',{})""".format(workflow_id,start_time,datetime.now(),current_run_id)
            b=cursor.execute(query)
            conn.commit()
        logger.info("Applying Data Profile success")
        
        container = ContainerClient.from_container_url(
                container_url=f"https://{cs.AZURE_EXTRACTOR_CONFIG_ACCOUNT}.blob.core.windows.net/{cs.AZURE_EXTRACTOR_CONFIG_CONTAINER}?",
                credential=cs.AZURE_EXTRACTOR_CONFIG_SAS_TOKEN)
        log_file=container.get_blob_client("workflow_log/{}_{}/{}/{}_{}".format(workflow_name, workflow_id, current_run_id,value_type,datetime.now()))
        log_file.upload_blob(output.getvalue(),overwrite=True)
        
        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(e)
        logger.info("Applying Data Profile Failed")
        query = """UPDATE workflow_details SET last_run_status='Failed' , active_indicator='N',last_run_id={} WHERE workflow_name='{}'""".format(current_run_id,workflow_name)
        cursor.execute(query)
        conn.commit()
        check_source=cursor.execute("""select * from workflow_execution_details where step_name='data_profile_Step' and run_id={} and workflow_id_id={}""".format(current_run_id, workflow_id))
        if check_source==1:
            query="""UPDATE workflow_execution_details SET step_completion_status ='Failed',step_started='{}',step_completion='{}' where step_name='data_profile_Step' and run_id={} and workflow_id_id={}""".format(start_time,datetime.now(),current_run_id,workflow_id)
            b=cursor.execute(query)
            conn.commit()
        else:
            query="""INSERT INTO workflow_execution_details (step_name,step_completion_status,workflow_id_id,step_started,step_completion,run_id)
            VALUES ('data_profile_Step','Failed', {},'{}','{}',{})""".format(workflow_id,start_time,datetime.now(),current_run_id)
            b=cursor.execute(query)
            conn.commit()
        
        container = ContainerClient.from_container_url(
                container_url=f"https://{cs.AZURE_EXTRACTOR_CONFIG_ACCOUNT}.blob.core.windows.net/{cs.AZURE_EXTRACTOR_CONFIG_CONTAINER}?",
                credential=cs.AZURE_EXTRACTOR_CONFIG_SAS_TOKEN)
        log_file=container.get_blob_client("workflow_log/{}_{}/{}/{}_{}".format(workflow_name, workflow_id, current_run_id,value_type,datetime.now()))
        log_file.upload_blob(output.getvalue(),overwrite=True)
        
        cursor.close()
        conn.close()
        raise ValueError('Applying Data Profile Failed')
    logging.shutdown()
data_profiling()
    


def write_to_target():   
    start_time=datetime.now()
    value_type='write_to_target'
    # connect to metadata
    conn = pymysql.connect(host=host, port=port, user=user_name, password=password, db=database, charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()

    # fetch current id
    cursor.execute("""select max(run_id) as current_run_id from workflow_execution_details
                        where workflow_id_id='{}'""".format(workflow_id))
    current_run_id=cursor.fetchone()
    current_run_id=current_run_id['current_run_id']
    
    
    # write log to azure container
    output = io.StringIO()
    handler = logging.StreamHandler(output)
    handler.setFormatter(formatter)
    logger = logging.getLogger('root')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
        

    # fetch workflow details
    query = """select workflow_id,target_details,epoch_id,target_load_type,target_load_details,source_load_type,source_load_details from workflow_details
                where workflow_id='{}'""".format(workflow_id)
    cursor.execute(query)
    conn.commit()
    record = cursor.fetchone()
    logger.info('metadata query :' + query)
    logger.info(record)
    target_details = record['target_details']
    target_details = json.loads(target_details)

    # fetch connection details
    cursor.execute("""select connection_details,password from connection_details
                    where connection_id='{}'""".format(target_details['target_connection_id']))
    target_connection_details = cursor.fetchone()
    conn.commit()
    target_connection = target_connection_details['connection_details']
    target_connection = json.loads(target_connection)

    # epoch_id
    now = datetime.now(timezone(local_timezone))
    current_epoch_id= int(time.mktime(now.timetuple()))
    logger.info("Initiating Loading the Data to Target databricks")
    target_load_details=record['target_load_details']
    target_load_details=json.loads(target_load_details)
    
    current_database=target_load_details['current_database']
    history_database=target_load_details['history_database']
    current_table=target_load_details['current_table']
    history_table=target_load_details['history_table']
    
    try: 
        logger.info('Initiating Write to target')    
        
        SparkContext.setSystemProperty('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        SparkContext.setSystemProperty('spline.mode',cs.SPLINE_MODE)
        SparkContext.setSystemProperty("spark.sql.queryExecutionListeners", "za.co.absa.spline.harvester.listener.SplineQueryExecutionListener")
        SparkContext.setSystemProperty('spline.producer.url', cs.SPLINE_PRODUCER_URL)
        SparkContext._jvm.za.co.absa.spline.harvester.SparkLineageInitializer.enableLineageTracking(spark._jsparkSession)
        spark.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs",cs.Legacy_Set_Command_Rejects)
        spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation",cs.Legacy_Allow_Creating_Managed_Table)
        spark.conf.set("spark.sql.legacy.timeParserPolicy",cs.Legacy_Time_Parser_Policy)
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

        
        spark.conf.set(f"fs.azure.sas.{cs.AZURE_TEMP_BLOB_CONTAINER}.{cs.AZURE_TEMP_BLOB_ACCOUNT}.blob.core.windows.net",cs.AZURE_TEMP_BLOB_TOKEN)
        
        if dq_checks_flag=='Y':
            file_name_erroneous = "dq_erroneous_{}_{}".format(workflow_name, current_run_id)
    
            file_name_correct = "dq_correct_{}_{}".format(workflow_name, current_run_id)
    
            source_df = spark.read.parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, file_name_correct))
            source_df=source_df.withColumn("epoch_id",lit(current_epoch_id))
            
            erroneous_data = spark.read.parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, file_name_erroneous))
    
            quarantine_data = erroneous_data.filter(col("action").rlike("quarantine")).drop("action")
            
            pd_quarantine_data = quarantine_data.toPandas()
            
            fail_job = erroneous_data.filter(col("action").rlike("failjob"))
    
            if fail_job.count() == 0:
                
                source_df=source_df.withColumn("etl_Create_date",current_timestamp())
                source_df=source_df.withColumn("etl_Update_date",current_timestamp())
    
                if record['epoch_id'] == None:
                
                    # Target run status
                    target_column_details = {"target_column_count":len(source_df.columns)}
                    target_row_details = {"target_row_count":source_df.count()}
                
                    source_df.write.mode('overwrite').option("compression",'Snappy').format('parquet').partitionBy(['epoch_id']).saveAsTable(f"{current_database}.{current_table}")
                else:
                    target_df_hist=spark.sql(f"select * from {current_database}.{current_table}")
                    target_df_hist=target_df_hist.withColumn("epoch_id",lit(record['epoch_id']))
                    target_df_hist.write.mode('append').option("compression",'Snappy').format('parquet').partitionBy(['epoch_id']).saveAsTable(f"{history_database}.{history_table}")
                    
                    # Target run status
                    target_column_details = {"target_column_count":len(source_df.columns)}
                    target_row_details = {"target_row_count":source_df.count()}
                    
                    source_df.write.mode('overwrite').option("compression",'Snappy').format('parquet').partitionBy(['epoch_id']).saveAsTable(f"{current_database}.{current_table}")
                    spark.sql(f"refresh table {current_database}.{current_table}")
                    spark.sql(f"MSCK REPAIR TABLE {current_database}.{current_table}")
                    spark.sql(f"refresh table {history_database}.{history_table}")
                    spark.sql(f"MSCK REPAIR TABLE {history_database}.{history_table}")
                    spark.sql(f"Alter table {current_database}.{current_table} drop IF EXISTS partition (epoch_id={record['epoch_id']})")
            
                
                if quarantine_data.count() > 0:
                    quarantine_data.write.mode("overwrite").format("parquet").saveAsTable(f"{current_database}.{current_table}_quarantine_data")
                
    
            elif quarantine_data.count() > 0:
                
                if quarantine_data.count() > 0:
                    quarantine_data.write.mode("overwrite").format("parquet").saveAsTable(f"{current_database}.{current_table}_quarantine_data")
                  
            else:
                logger.info('Data not loaded to target due to action fail job in DQ step')
                pass
        elif transformation_flag=='Y':
            if dq_checks_flag=='N':
                file_name = "intermediate_{}_{}".format(workflow_name, current_run_id)
                source_df = spark.read.parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, file_name))
                source_df=source_df.withColumn("epoch_id",lit(current_epoch_id))
                
                source_df=source_df.withColumn("etl_Create_date",current_timestamp())
                source_df=source_df.withColumn("etl_Update_date",current_timestamp())
    
                if record['epoch_id'] == None:
                
                    # Target run status
                    target_column_details = {"target_column_count":len(source_df.columns)}
                    target_row_details = {"target_row_count":source_df.count()}
                
                    source_df.write.mode('overwrite').option("compression",'Snappy').format('parquet').partitionBy(['epoch_id']).saveAsTable(f"{current_database}.{current_table}")
                else:
                    target_df_hist=spark.sql(f"select * from {current_database}.{current_table}")
                    target_df_hist=target_df_hist.withColumn("epoch_id",lit(record['epoch_id']))
                    target_df_hist.write.mode('append').option("compression",'Snappy').format('parquet').partitionBy(['epoch_id']).saveAsTable(f"{history_database}.{history_table}")
                    
                    # Target run status
                    target_column_details = {"target_column_count":len(source_df.columns)}
                    target_row_details = {"target_row_count":source_df.count()}
                    
                    source_df.write.mode('overwrite').option("compression",'Snappy').format('parquet').partitionBy(['epoch_id']).saveAsTable(f"{current_database}.{current_table}")
                    spark.sql(f"refresh table {current_database}.{current_table}")
                    spark.sql(f"MSCK REPAIR TABLE {current_database}.{current_table}")
                    spark.sql(f"refresh table {history_database}.{history_table}")
                    spark.sql(f"MSCK REPAIR TABLE {history_database}.{history_table}")
                    spark.sql(f"Alter table {current_database}.{current_table} drop IF EXISTS partition (epoch_id={record['epoch_id']})")
            
        else:
            if dq_checks_flag=='N':
                file_name = "{}_{}".format(workflow_name, current_run_id)
                source_df = spark.read.parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, file_name))
                source_df=source_df.withColumn("epoch_id",lit(current_epoch_id))
                
                source_df=source_df.withColumn("etl_Create_date",current_timestamp())
                source_df=source_df.withColumn("etl_Update_date",current_timestamp())
    
                if record['epoch_id'] == None:
                
                    # Target run status
                    target_column_details = {"target_column_count":len(source_df.columns)}
                    target_row_details = {"target_row_count":source_df.count()}
                
                    source_df.write.mode('overwrite').option("compression",'Snappy').format('parquet').partitionBy(['epoch_id']).saveAsTable(f"{current_database}.{current_table}")
                else:
                    target_df_hist=spark.sql(f"select * from {current_database}.{current_table}")
                    target_df_hist=target_df_hist.withColumn("epoch_id",lit(record['epoch_id']))
                    target_df_hist.write.mode('append').option("compression",'Snappy').format('parquet').partitionBy(['epoch_id']).saveAsTable(f"{history_database}.{history_table}")
                    
                    # Target run status
                    target_column_details = {"target_column_count":len(source_df.columns)}
                    target_row_details = {"target_row_count":source_df.count()}
                    
                    source_df.write.mode('overwrite').option("compression",'Snappy').format('parquet').partitionBy(['epoch_id']).saveAsTable(f"{current_database}.{current_table}")
                    spark.sql(f"refresh table {current_database}.{current_table}")
                    spark.sql(f"MSCK REPAIR TABLE {current_database}.{current_table}")
                    spark.sql(f"refresh table {history_database}.{history_table}")
                    spark.sql(f"MSCK REPAIR TABLE {history_database}.{history_table}")
                    spark.sql(f"Alter table {current_database}.{current_table} drop IF EXISTS partition (epoch_id={record['epoch_id']})")
            
        logger.info("Data has been Load to Target  epoch_id={}".format(current_epoch_id))
        
        if record['source_load_type']=='increment':
            increment_details_1 = record['source_load_details']
            increment_details_2 = json.loads(increment_details_1)
            increment_details_3 = increment_details_2['increment_data']
            final_json=[]
            for i in increment_details_3:
                if i['consider_max_val']==True:
                    df1=spark.read.parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/increment_details/".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name))
                    value1=df1.select(col(i['key_column'])).collect()[0][0]
                    i['value']=str(value1)
                    final_json.append(i)
                else:
                    final_json.append(i)
            final_increment_value={}
            final_increment_value['increment_data']=final_json
            final_increment_value = json.dumps(final_increment_value)
            query="""update workflow_details set source_load_details = '{}' where workflow_id = {}""".format(final_increment_value,workflow_id)
            cursor.execute(query)
            conn.commit()
        
        # GET RUN STATUS 
        run_details_query = """select max(run_details) as run_details from workflow_execution_details where workflow_id_id='{}' and run_id='{}'""".format(workflow_id,current_run_id)
        cursor.execute(run_details_query)
        run_details_record=cursor.fetchone()
        dq_run_details=json.loads(run_details_record['run_details'])

        # Target run status
        run_details_dict.update({**dq_run_details,**target_column_details, **target_row_details})
        run_details = json.dumps(run_details_dict)

        query = """UPDATE workflow_details SET last_run_status='success' , active_indicator='N',last_run_id={},epoch_id={} WHERE workflow_id='{}'""".format(current_run_id,current_epoch_id,workflow_id)
        cursor.execute(query)
        conn.commit()
        check_source=cursor.execute("""select * from workflow_execution_details where step_name='Write_to_target' and run_id={} and workflow_id_id={}""".format(current_run_id,workflow_id))
        if check_source==1:
            query="""UPDATE workflow_execution_details SET step_completion_status ='success',run_details='{}' where step_name='Write_to_target' and run_id={} and workflow_id_id={}""".format(run_details,current_run_id,workflow_id)
            cursor.execute(query)
            conn.commit()
        else:
            query="""INSERT INTO workflow_execution_details (step_name,step_completion_status,workflow_id_id,step_started,step_completion,run_id,run_details)
            VALUES ('Write_to_target','success', {},'{}','{}',{},'{}')""".format(workflow_id,start_time,datetime.now(),current_run_id,run_details)
            cursor.execute(query)
            conn.commit()
        
        container = ContainerClient.from_container_url(
                container_url=f"https://{cs.AZURE_EXTRACTOR_CONFIG_ACCOUNT}.blob.core.windows.net/{cs.AZURE_EXTRACTOR_CONFIG_CONTAINER}?",
                credential=cs.AZURE_EXTRACTOR_CONFIG_SAS_TOKEN)
        log_file=container.get_blob_client("workflow_log/{}_{}/{}/{}_{}".format(workflow_name, workflow_id, current_run_id,value_type,datetime.now()))
        log_file.upload_blob(output.getvalue(),overwrite=True)
        
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(e)
        logger.info("load to target databricks Failed")

        # GET RUN STATUS 
        run_details_query = """select max(run_details) as run_details from workflow_execution_details where workflow_id_id='{}' and run_id='{}'""".format(workflow_id,current_run_id)
        cursor.execute(run_details_query)
        run_details_record=cursor.fetchone()
        dq_run_details=json.loads(run_details_record['run_details'])

        # ERROR LOG
        error_details = {"error_log":f"{e}"}
        run_details_dict.update({**dq_run_details,**error_details})
        run_details = json.dumps(run_details_dict)

        query = """UPDATE workflow_details SET last_run_status='Failed' , active_indicator='N',last_run_id={} WHERE workflow_name='{}'""".format(current_run_id,workflow_name)
        cursor.execute(query)
        conn.commit()
        check_source=cursor.execute("""select * from workflow_execution_details where step_name='Write_to_target' and run_id={} and workflow_id_id={}""".format(current_run_id,workflow_id))
        if check_source==1:
            query="""UPDATE workflow_execution_details SET step_completion_status ='Failed',step_started='{}',step_completion='{}',run_details='{}' where step_name='Write_to_target' and run_id={} and workflow_id_id={}""".format(start_time,datetime.now(),run_details,current_run_id,workflow_id)
            cursor.execute(query)
            conn.commit()
        else:
            query="""INSERT INTO workflow_execution_details (step_name,step_completion_status,workflow_id_id,step_started,step_completion,run_id,run_details)
            VALUES ('Write_to_target','Failed', {},'{}','{}',{},'{}')""".format(workflow_id,start_time,datetime.now(),current_run_id,run_details)
            cursor.execute(query)
            conn.commit()
        
        container = ContainerClient.from_container_url(
                container_url=f"https://{cs.AZURE_EXTRACTOR_CONFIG_ACCOUNT}.blob.core.windows.net/{cs.AZURE_EXTRACTOR_CONFIG_CONTAINER}?",
                credential=cs.AZURE_EXTRACTOR_CONFIG_SAS_TOKEN)
        log_file=container.get_blob_client("workflow_log/{}_{}/{}/{}_{}".format(workflow_name, workflow_id, current_run_id,value_type,datetime.now()))
        log_file.upload_blob(output.getvalue(),overwrite=True)
        
        cursor.close()
        conn.close()
        raise ValueError('load to target databricks failed')
    logging.shutdown()
write_to_target()   
    


def email_step(time_zone):  
   

    start_time=datetime.now()
    value_type='Email_report'
    # connect to metadata
    conn = pymysql.connect(host = host,port=port,user = user_name,password = password,db = database, charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor() 
     
    # fetch current id
    cursor.execute("""select max(run_id) as current_run_id from workflow_execution_details
                        where workflow_id_id='{}'""".format(workflow_id))
    current_run_id=cursor.fetchone()
    current_run_id=current_run_id['current_run_id']
    
    
    # write log to azure container
    output = io.StringIO()
    handler = logging.StreamHandler(output)
    handler.setFormatter(formatter)
    logger = logging.getLogger('root')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
        

    # fetch workflow details
    query = """select workflow_id,target_details,epoch_id,target_load_type,target_load_details,source_load_type,source_load_details from workflow_details
                where workflow_id='{}'""".format(workflow_id)
    cursor.execute(query)
    conn.commit()
    record = cursor.fetchone()

    logger.info('metadata query :' + query)
    logger.info(record)
    target_details = record['target_details']
    target_details = json.loads(target_details)
    
    cursor.execute("""select sd.schedule_start_time,wed.step_name,wed.step_completion_status from 
            schedule_details sd join workflow_execution_details wed on wed.workflow_id_id = sd.workflow_id_id 
            where wed.run_id = {} and sd.workflow_id_id = {}""".format(current_run_id, workflow_id))
            
    workflow_status = cursor.fetchall()

    target_load_details=record['target_load_details']
    target_load_details=json.loads(target_load_details)
    
    # fetch connection details
    cursor.execute("""select connection_details,password from connection_details
                    where connection_id='{}'""".format(target_details['target_connection_id']))
    target_connection_details = cursor.fetchone()
    conn.commit()
    target_connection = target_connection_details['connection_details']
    target_connection = json.loads(target_connection)

    current_database=target_load_details['current_database']
    history_database=target_load_details['history_database']
    current_table=target_load_details['current_table']
    history_table=target_load_details['history_table']
    

    logger.info('Generating DQ Report')
    try:
        spark = SparkSession.builder.appName(workflow_name).getOrCreate()
        
        spark.conf.set(f"fs.azure.sas.{cs.AZURE_TEMP_BLOB_CONTAINER}.{cs.AZURE_TEMP_BLOB_ACCOUNT}.blob.core.windows.net",cs.AZURE_TEMP_BLOB_TOKEN)
        
        def dq_tble_name(workflow_id):
            cursor.execute("""select source_details from workflow_details where workflow_id = '{}'""".format(workflow_id))
            data = json.loads(cursor.fetchall()[0]["source_details"])
            if data["connection_type"]==cs.EXTRACTOR_SOURCE_SYSTEMS.get("Linux File Server"):
                table_name = data["file_location"].split("/")[-1].split(".")[0]
            elif data["connection_type"]==cs.EXTRACTOR_SOURCE_SYSTEMS.get("Amazon SFTP") or data["connection_type"]==cs.EXTRACTOR_SOURCE_SYSTEMS.get("Azure Blob Storage"):
                table_name = data["file_location"].split("/")[-1].split(".")[0]
            elif data["connection_type"]=='Salesforce':
                table_name=data["object_name"]
            else:
                table_name = data["source_table"]
        
            return(table_name)
        
        tbl_name = dq_tble_name(workflow_id)
        ex_summ = ('', '', '', '', '')
        def workflow_ex_status():
            w_status = "Failed"
            if ex_summ[2].split(" ")[0]=="Failed":
                w_status = "Failed"
            else:
                for rec in workflow_status:
                    if rec['step_name']=='Write_to_target':
                        if rec['step_completion_status']=='success':
                            w_status = "Success"
                            break
                        elif rec['step_completion_status']=='Failed':
                            w_status = "Failed"
                            break
                    else:
                        continue
            return(w_status)
            
        w_status = workflow_ex_status()
        
        
        mail = mailer_module.Mailer(to=cs.to_mail_list)
        mail.build_subject_status(job_name = workflow_name, status = w_status, time_zone = time_zone)
        current_time = datetime.now().astimezone(timezone(time_zone)).strftime('%I:%M %p %Z')
        schd_time = workflow_status[0]["schedule_start_time"].astimezone(timezone(time_zone)).strftime('%I:%M %p %Z')
        mail.build_status_mail_body(workflow_name, schd_time, current_time, w_status)
        mail.send_email()
        time.sleep(1)
        

        if dq_checks_flag=='Y':

            file_name_erroneous = "dq_erroneous_{}_{}".format(workflow_name, current_run_id)
    
            file_name_correct = "dq_correct_{}_{}".format(workflow_name, current_run_id)
           
            source_file_name = "{}_{}".format(workflow_name, current_run_id)
    
            source_count = spark.read.parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, source_file_name)).count()

            file_name_correct = "dq_correct_{}_{}".format(workflow_name, current_run_id)
    
            try:
                target_count = spark.read.parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, file_name_correct)).count()
            except:
                target_count = 0
            
            
            try:
                quarantined_count = spark.table(f"{current_database}.{current_table}_quarantine_data").count()
            except:
                quarantined_count = 0
        

            print("#############", quarantined_count, record["epoch_id"], "#############################")
            
            
            url=current_database+'.'+current_table+'_quarantine_data'
            
            
            file_name_errors = "dq_validation_{}_{}".format(workflow_name, current_run_id)
            
            validation_summary = spark.read.parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, file_name_errors))
            
            def dq_execution_summary(workflow_id):
                job = 'No'
                tick = 'No'
                cursor.execute("""select dq_detail from dq_details 
                    where workflow_id_id = '{}'""".format(workflow_id))
                data = cursor.fetchall()
                for dq_check in data:
                    checks = json.loads(dq_check["dq_detail"])[0]
                    if checks["failJob"] == False:
                        continue
                    else:
                        job = "Yes"
                    if checks["ticket"] == False:
                        continue
                    else:
                        tick = "Yes"
                if quarantined_count > 0 and job == "No":
                    status = "Passed with Quarantine Data"
                elif quarantined_count > 0 and job == "Yes":
                    status = "Failed with Quarantine Data"
                elif quarantined_count == 0 and job == "Yes":
                    status = "Failed with No Quarantine Data"
                else:
                    status = "Passed with No Quarantine Data"
                current_time = datetime.now().astimezone(timezone(time_zone)).strftime('%I:%M %p %Z')
                return (tbl_name, current_time, status, tick, job)
            
            ex_summ = dq_execution_summary(workflow_id)
            
            
            pdf = validation_summary.toPandas()  
            mail = mailer_module.Mailer(to=cs.to_mail_list)
            mail.build_subject(table_name=ex_summ[0], dq_status=ex_summ[2].split(" ")[0], time_zone=time_zone)
            mail.build_mail_body([ex_summ], validation_summary.take(10), (source_count, target_count, quarantined_count), url, pdf)
            mail.send_email()
            time.sleep(1)
        
        
        if data_profile_flag=='Y':
            file_name_data_profile = "data_profile_{}_{}".format(workflow_name, current_run_id)
    
            dpdf = spark.read.parquet("wasbs://{}@{}.blob.core.windows.net/ingestion_temp_{}_{}/{}".format(cs.AZURE_TEMP_BLOB_CONTAINER,cs.AZURE_TEMP_BLOB_ACCOUNT,current_run_id, workflow_name, file_name_data_profile))
            dpdf = dpdf.drop(col("action"))
                       
            
            if dpdf.count() > 0:
                
                pdf = dpdf.toPandas()
                mail = mailer_module.Mailer(to=cs.to_mail_list)
                mail.build_subject_dataprofile(tbl_name, time_zone=time_zone)
                current_time = datetime.now().astimezone(timezone(time_zone)).strftime('%I:%M %p %Z')
                schd_time = workflow_status[0]["schedule_start_time"].astimezone(timezone(time_zone)).strftime('%I:%M %p %Z')
                mail.build_mail_body_dataprofile(tbl_name, schd_time, current_time, pdf)
                mail.send_email()
                time.sleep(1)
        
                # dq_mail(dpdf, "Data Profile Report", file_name_data_profile)
            
        
        if dq_table_check_flag=='Y':
            query="""select dq_table_check,dq_table_result from dq_table_details where workflow_id={}""".format(workflow_id)
            cursor.execute(query)
            dq_table_details = cursor.fetchone()
            dq_check_table_details=dq_table_details['dq_table_check']
            dq_check_table_details=json.loads(dq_check_table_details)
            source_target_control_file_mail='F'
            source_target_mail='F'
            source_target_threshold_mail='F'
            for i in dq_check_table_details:
                for j in i['subStrategy']:
                    if str.lower(j)=='source_target_control_file_based_count_validation':
                        if i['email']==True:
                            source_target_control_file_mail='T'
                    elif str.lower(j)=='source_target_table_count_validation':
                        if i['email']==True:
                            source_target_mail='T'
                    elif str.lower(j)=='source_target_threshold_based_count_validation':
                        if i['email']==True:
                            threshold_value=i['threshold_value']
                            source_target_threshold_mail='T'
            count_details=dq_table_details['dq_table_result']
            count_details=json.loads(count_details)
            if 'source_target_control_file_based_count_validation' in count_details and source_target_control_file_mail=='T':
                if count_details['source_target_control_file_based_count_validation']==False :
                    
                    mail = mailer_module.Mailer(to=cs.to_mail_list)
                    mail.build_subject_status(job_name = workflow_name, status = 'Failed', time_zone = time_zone)
                    current_time = datetime.now().astimezone(timezone(time_zone)).strftime('%I:%M %p %Z')
                    schd_time = workflow_status[0]["schedule_start_time"].astimezone(timezone(time_zone)).strftime('%I:%M %p %Z')
                    mail.html_body_generic_format(dict_of_tables=count_details,type_of_validation='source_target_control_file_based_count_validation')
                    mail.send_email()
                    time.sleep(1)
        
            if 'source_target_threshold_based_count_validation' in count_details and source_target_threshold_mail=='T':
                if count_details['source_target_threshold_based_count_validation']==False :
                    
                    mail = mailer_module.Mailer(to=cs.to_mail_list)
                    mail.build_subject_status(job_name = workflow_name, status = 'Failed', time_zone = time_zone)
                    current_time = datetime.now().astimezone(timezone(time_zone)).strftime('%I:%M %p %Z')
                    schd_time = workflow_status[0]["schedule_start_time"].astimezone(timezone(time_zone)).strftime('%I:%M %p %Z')
                    mail.html_body_generic_format(dict_of_tables=count_details,type_of_validation='source_target_threshold_based_count_validation',value=threshold_value)
                    mail.send_email()
                    time.sleep(1)
        
            if 'source_target_table_count_validation' in count_details and source_target_mail=='T':
                if count_details['source_target_table_count_validation']==False :
                    
                    mail = mailer_module.Mailer(to=cs.to_mail_list)
                    mail.build_subject_status(job_name = workflow_name, status = 'Failed', time_zone = time_zone)
                    current_time = datetime.now().astimezone(timezone(time_zone)).strftime('%I:%M %p %Z')
                    schd_time = workflow_status[0]["schedule_start_time"].astimezone(timezone(time_zone)).strftime('%I:%M %p %Z')
                    mail.html_body_generic_format(dict_of_tables=count_details,type_of_validation='source_target_table_count_validation')
                    mail.send_email()
                    time.sleep(1)
        

        
        
        if w_status=='Failed':
            raise Exception('Extractor failed')
        
        if w_status != "Failed": 
            
            container = ContainerClient.from_container_url(
            container_url=f"https://{cs.AZURE_TEMP_BLOB_ACCOUNT}.blob.core.windows.net/{cs.AZURE_TEMP_BLOB_CONTAINER}?",
            credential=cs.AZURE_TEMP_BLOB_TOKEN)
            folder_names=[]
            for blob in container.list_blobs(name_starts_with="ingestion_temp_{}_{}".format(current_run_id,workflow_name)):
                file_length=blob.name.split('/')
                if len(file_length)>2:
                    container.delete_blob(blob.name)
                elif len(file_length)>1:
                    folder_names.append(blob.name)
            for i in folder_names:
                container.delete_blob(i)
            container.delete_blob("ingestion_temp_{}_{}".format(current_run_id,workflow_name))
        

        check_source=cursor.execute("""select * from workflow_execution_details where step_name='DQ_Report' and run_id={} and workflow_id_id={}""".format(current_run_id,workflow_id))
        if check_source==1:
            query="""UPDATE workflow_execution_details SET step_completion_status ='success' where step_name='DQ_Report' and run_id={} and workflow_id_id={}""".format(current_run_id,workflow_id)
            b=cursor.execute(query)
            conn.commit()
        else:
            query="""INSERT INTO workflow_execution_details (step_name,step_completion_status,workflow_id_id,step_started,step_completion,run_id)
            VALUES ('DQ_Report','success', {},'{}','{}',{})""".format(workflow_id,start_time,datetime.now(),current_run_id)
            b=cursor.execute(query)
            conn.commit()
        logger.info("Generating DQ Report success")
        
        container = ContainerClient.from_container_url(
                container_url=f"https://{cs.AZURE_EXTRACTOR_CONFIG_ACCOUNT}.blob.core.windows.net/{cs.AZURE_EXTRACTOR_CONFIG_CONTAINER}?",
                credential=cs.AZURE_EXTRACTOR_CONFIG_SAS_TOKEN)
        log_file=container.get_blob_client("workflow_log/{}_{}/{}/{}_{}".format(workflow_name, workflow_id, current_run_id,value_type,datetime.now()))
        log_file.upload_blob(output.getvalue(),overwrite=True)
        
        cursor.close()
        conn.close()

    except Exception as e:
        print(e)
        logger.error(e)
        logger.info("Generating DQ Report Failed")
        query = """UPDATE workflow_details SET last_run_status='Failed' , active_indicator='N',last_run_id={} WHERE workflow_name='{}'""".format(current_run_id,workflow_name)
        cursor.execute(query)
        conn.commit()
        check_source=cursor.execute("""select * from workflow_execution_details where step_name='DQ_Step' and run_id={} and workflow_id_id={}""".format(current_run_id,workflow_id))
        if check_source==1:
            query="""UPDATE workflow_execution_details SET step_completion_status ='Failed',step_started='{}',step_completion='{}' where step_name='DQ_Step' and run_id={} and workflow_id_id={}""".format(start_time,datetime.now(),current_run_id,workflow_id)
            b=cursor.execute(query)
            conn.commit()
        else:
            query="""INSERT INTO workflow_execution_details (step_name,step_completion_status,workflow_id_id,step_started,step_completion,run_id)
            VALUES ('DQ_Report','Failed', {},'{}','{}',{})""".format(workflow_id,start_time,datetime.now(),current_run_id)
            b=cursor.execute(query)
            conn.commit()
        raise ValueError('Generating DQ Report Failed')
        
        container = ContainerClient.from_container_url(
                container_url=f"https://{cs.AZURE_EXTRACTOR_CONFIG_ACCOUNT}.blob.core.windows.net/{cs.AZURE_EXTRACTOR_CONFIG_CONTAINER}?",
                credential=cs.AZURE_EXTRACTOR_CONFIG_SAS_TOKEN)
        log_file=container.get_blob_client("workflow_log/{}_{}/{}/{}_{}".format(workflow_name, workflow_id, current_run_id,value_type,datetime.now()))
        log_file.upload_blob(output.getvalue(),overwrite=True)
        
        cursor.close()
        conn.close()
    logging.shutdown()
email_step(local_timezone)
    

