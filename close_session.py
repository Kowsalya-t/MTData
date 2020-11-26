from settings import DB_CONNECTION
from redshift_utils import RedshiftDataManager
import logging
from datetime import datetime

#Current time
now = datetime.now()

query = "SELECT process, user_name, session_dur, idle_dur FROM inactive_sessions where idle_dur >= 30*60"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def close_idle_redshift_sessions(event, context):

   try:
       conn = RedshiftDataManager.get_conn(DB_CONNECTION)
       conn.autocommit = True
   except:
       logger.error("ERROR: Unexpected error: Could not connect to Redshift cluster.")   
       sys.exit()

   logger.info("SUCCESS: Connection to Redshift cluster succeeded")

   with conn.cursor() as cur:
       cur.execute(query)
       row_count = cur.rowcount
       if row_count >=1:
           result = cur.fetchall()
           for row in result:
                print("terminating session with pid %s that has been idle for %d seconds at %s" % (row[0],row[3],now))
                cur.execute("SELECT PG_TERMINATE_BACKEND(%s);" % (row[0]))
           conn.close()
       else:
           conn.close()