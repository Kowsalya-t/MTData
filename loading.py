from redshift_utils import Messages
from redshift_utils import ScriptReader
from redshift_utils import RedshiftDataManager
from settings import get_pwd

import json, boto3, os, sys

# Get the service resource
sqs = boto3.resource('sqs')
sqs_client = boto3.client('sqs')
redshift_client = boto3.client('redshift')
sns = boto3.client('sns')

Environment =  os.environ['environment']
iamRole = os.environ['iam']
snsTopic = os.environ['snsTopic']

hostname = redshift_client.describe_clusters(
		ClusterIdentifier='mtdata-datalake-cluster-'+Environment
)['Clusters'][0]['Endpoint']['Address']

target_table = 'public.mtdata_data_evnt'
iam = iamRole 
format_type = 'CSV'

def mtdata_datalake_cp_redshift(event, context):   
	print("event :" , event)
	
	print("hostname :", hostname)
	
	try:
		receipt_handle = event['Records'][0]['receiptHandle']
		body = event['Records'][0]['body']
		bucket = json.loads(body)['Records'][0]['s3']['bucket']['name']
		file = json.loads(body)['Records'][0]['s3']['object']['key']
		filename_new    = file.replace('%3D','=').replace('+',' ').replace(' ','').replace('%26','&')
		
		script = 'COPY ' + target_table + ' from \'s3://' + bucket + '/' + filename_new + '\' iam_role \'' + iam + '\' FORMAT AS ' + format_type + ' IGNOREHEADER 1 dateformat \'auto\';'
		print("Script : ", script)
		
		DB_CONNECTION = {
		'db_host':  hostname, 
		'db_name': Environment, 
		'db_username': 'awsuser',
		'db_password': get_pwd.pwd()
	}
		print (DB_CONNECTION)
		result = RedshiftDataManager.run_update(script, DB_CONNECTION)
		
		TargetArn_str = snsTopic
		
		if result[1] != None:
			message = str(result[1])
			response = sns.publish(
			TargetArn=TargetArn_str,
			Message=message,
			Subject=context.function_name,
			MessageStructure='string'
			)

		if result[1] == None:
			print("Deleting Queue Message ...")
			message  = sqs.Message(sqs_client.get_queue_url(QueueName = 'mtdata-datalake-staging-'+Environment)['QueueUrl'],receipt_handle)
			response = message.delete()
		
	except:
		try:
			print("In except")
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