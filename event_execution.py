import boto3,json,os

Environment =  os.environ['environment']

client = boto3.client('sqs')
def sqs_add(event, context):
	
	print(json.dumps(event,default=str))
	print("Context :" , json.dumps(context,default=str))
	bucket_name = event['Records'][0]['s3']['bucket']['name']
	filename = event['Records'][0]['s3']['object']['key']
	print("bucket_name :", bucket_name)
	if 'broker-messages' in bucket_name:
		
		response = client.send_message(
    	QueueUrl= client.get_queue_url(QueueName = 'msg_brkr_q_'+Environment)['QueueUrl'],
    	MessageBody=json.dumps(event,default=str),
    	MessageAttributes={}
	)
	
def sqs_add_datalake(event,context):
	
	print(json.dumps(event,default=str))
	print("Context :" , json.dumps(context,default=str))
	bucket_name = event['Records'][0]['s3']['bucket']['name']
	filename = event['Records'][0]['s3']['object']['key']
	print("bucket_name :", bucket_name)
	if 'mtdata-datalake' in bucket_name and 'datalake_lnd' in filename:
		
		response = client.send_message(
    	QueueUrl= client.get_queue_url(QueueName = 'mtdata-datalake-landing-'+Environment)['QueueUrl'],
    	MessageBody=json.dumps(event,default=str),
    	MessageAttributes={}
		)
	else:
		if 'mtdata-datalake'  in bucket_name and 'datalake_stg' in filename:
			
			response = client.send_message(
			QueueUrl= client.get_queue_url(QueueName = 'mtdata-datalake-staging-'+Environment)['QueueUrl'],
			MessageBody=json.dumps(event,default=str),
			MessageAttributes={}
			)
			#Remove the below block for other env.
		else:
			
			response = client.send_message(
    		QueueUrl='https://sqs.ap-southeast-2.amazonaws.com/036843008788/msg_bus_q',
    		MessageBody=json.dumps(event,default=str),
			MessageAttributes={}
			)
	print("Response : ", response)
	return("Success")