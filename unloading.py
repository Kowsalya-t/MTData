from redshift_utils import Messages
from redshift_utils import ScriptReader
from redshift_utils import RedshiftDataManager
from settings import INS_SCRIPT_PATH 
from settings import DEL_SCRIPT_PATH
from settings import AUDIT_SCRIPT_PATH
from settings import DB_CONNECTION
import json, boto3, sys , os
from datetime import datetime

glue = boto3.client('glue')
sns = boto3.client('sns')

# Get the service resource
sqs = boto3.resource('sqs')

Environment =  os.environ['environment']
iamRole = os.environ['iam']
s3Prefix = os.environ['unloadPath']
snsTopic = os.environ['snsTopic']


def mtdata_unload_redshift_s3(event, context):
    
    now  = datetime.now()    
    time = now.strftime("%y_%m_%d_%H%M%S") 
    date = now.strftime("%Y%m%d%H%M")
	
    query = '(\'select * from MTDATA_DATA_EVNT_TGT where arrivaltimestamp > (select nvl(max(unloaded_timestamp),to_date(' + '\'' + '\'1900-01-01\'' + '\'' + ',' + '\'' + '\'YYYY-MM-DD\'' + '\'' + ')) from interface_process_control) \')'
	
    print("Environment : ", Environment)
	
    s3_prefix = "'" + s3Prefix + "'"
    iam = "'" + iamRole + "'"
    
    try:
        print("inserting ....")
        ins_script = ScriptReader.get_script(INS_SCRIPT_PATH)
        RedshiftDataManager.run_update(ins_script, DB_CONNECTION)    
        
        print("unloading....")
        unload_script = 'unload '+ query + ' to ' +  s3_prefix + ' iam_role ' + iam +' PARQUET PARTITION BY(CustomerReferenceCode,fleetid,vehicleid,currentdatetime) ALLOWOVERWRITE'
        result = RedshiftDataManager.run_update(unload_script, DB_CONNECTION)
        
        print(result)
        
        if result[1] == None:
         print("inserting audit ... ")
         ins_audit_script = ScriptReader.get_script(AUDIT_SCRIPT_PATH)
         RedshiftDataManager.run_update(ins_audit_script, DB_CONNECTION)
        
         print("deleteing ... ")
         del_script = ScriptReader.get_script(DEL_SCRIPT_PATH)
         RedshiftDataManager.run_update(del_script, DB_CONNECTION)
        
        print("start crawler ...")
        if Environment in ('dev', 'prod'):
           response = glue.start_crawler(
           Name='msg_brkr_redshift_unload')

        else:
           response = glue.start_crawler(
           Name='msg_brkr_redshift_unload_qa')
           
    except:    
        try:
            message = str(sys.exc_info())
            TargetArn_str = snsTopic
            
            response = sns.publish(
                TargetArn=TargetArn_str,
                Message=message,
                Subject=context.function_name,
                MessageStructure='string'
            )
            
        except:    
            print("Failed :-( :", sys.exc_info())
        
    return "Success"