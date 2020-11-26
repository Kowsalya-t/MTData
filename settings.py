import os
###############################################
# Script settings and constants.
###############################################

INS_SCRIPT_PATH = 'script.sql'
DEL_SCRIPT_PATH = 'del_script.sql'
AUDIT_SCRIPT_PATH = 'insert_audit_script.sql'
CREATE_SCRIPT_PATH = 'create_script.sql'

if os.environ['environment'] in ("prod"):
	DB_CONNECTION = {
		'db_host': 'mtdata-datalake-cluster-'+os.environ['environment']+'.cskwxmhzvf7m.ap-southeast-2.redshift.amazonaws.com', 
		'db_name': os.environ['environment'], 
		'db_username': 'awsuser',
		'db_password': 'Awsuser1'
	}
else:
	DB_CONNECTION = {
		'db_host': 'mtdata-datalake-cluster-'+os.environ['environment']+'.c39ncvggo92u.ap-southeast-2.redshift.amazonaws.com', #hostname might have to change
		'db_name': os.environ['environment'], 
		'db_username': 'awsuser',
		'db_password': 'Awsuser1'
	}

Username = 'awsuser'

class get_pwd(object):
	def pwd():
		if os.environ['environment'] == 'dev' :
			return 'Awsuser1'
		else:
			return 'Awsuser1'