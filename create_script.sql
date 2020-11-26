drop table public.mtdata_data_evnt;
CREATE TABLE public.mtdata_data_evnt
(
	customerreferencecode VARCHAR(10) NOT NULL
	,instancename VARCHAR(25) 
	,fleetid INTEGER NOT NULL
	,vehicleid INTEGER NOT NULL
	,deviceid INTEGER 
	,devicetype VARCHAR(50) 
	,serialnumber varchar(20) 
	,devicetime TIMESTAMP
	,gpstime TIMESTAMP
	,arrivaltimestamp TIMESTAMP
	,latitude FLOAT
	,longitude FLOAT
	,flags INTEGER 
	,direction INTEGER 
	,speed FLOAT
	,maxspeed FLOAT
	,inputstatus INTEGER 
	,statusflag INTEGER 
	,outputstatus INTEGER 
	,buttonstatus INTEGER 
	,lightstatus INTEGER 
	,sparestatus INTEGER 
	,satellites_altitude INTEGER 
	,satellites_noinview INTEGER 
	,satellites_hdop INTEGER 
	,speedaccuracydp INTEGER 
	,gpsodometer INTEGER 
	,drivername VARCHAR(100) 
	,driverlogin INTEGER 
	,driverpin INTEGER 
	,cardtype INTEGER 
	,drivercarddata BOOLEAN
	,speedaccuracydecimalplaces FLOAT
	,maxspeedaccuracydecimalplaces FLOAT
	,ecmflags INTEGER 
	,hasgforce BOOLEAN
	,cardidaslong INTEGER 
	,unitstatus INTEGER 
	,currentsetpointid INTEGER 
	,currentsetpointgroup INTEGER 
	,currentsetpointnumber INTEGER 
	,currentvehicleroutescheduleid INTEGER 
	,currentroutecheckpointindex INTEGER 
	,hdop FLOAT
	,numofsattelites INTEGER
	,coolanttemperature	FLOAT
  ,oiltemperature	FLOAT
  ,oilpressure	FLOAT
  ,gear	INTEGER
  ,maxrpm	INTEGER
  ,brakeapplications	FLOAT
  ,gforcefront	FLOAT
  ,gforceback	FLOAT
  ,gforceleftright	FLOAT
  ,schoolzone	boolean
  ,isprivate	boolean
  ,roadtype	INTEGER
  ,streetnumber	VARCHAR(10)
  ,street	varchar(100)
  ,suburb	varchar(100)
  ,city	varchar(100)
  ,state	varchar(100)
  ,country	varchar(100)
  ,postcode	INTEGER
  ,speedlimit	FLOAT
);

----------------------------------------

drop table public.mtdata_data_evnt_tgt;
CREATE TABLE IF NOT EXISTS public.mtdata_data_evnt_tgt
(
	customerreferencecode VARCHAR(10) NOT NULL
	,instancename VARCHAR(25) 
	,fleetid INTEGER NOT NULL
	,vehicleid INTEGER NOT NULL
	,deviceid INTEGER 
	,devicetype VARCHAR(50) 
	,serialnumber varchar(20) 
	,devicetime TIMESTAMP
	,gpstime TIMESTAMP
	,arrivaltimestamp TIMESTAMP
	,latitude FLOAT
	,longitude FLOAT
	,flags INTEGER 
	,direction INTEGER 
	,speed FLOAT
	,maxspeed FLOAT
	,inputstatus INTEGER 
	,statusflag INTEGER 
	,outputstatus INTEGER 
	,buttonstatus INTEGER 
	,lightstatus INTEGER 
	,sparestatus INTEGER 
	,satellites_altitude INTEGER 
	,satellites_noinview INTEGER 
	,satellites_hdop INTEGER 
	,speedaccuracydp INTEGER 
	,gpsodometer INTEGER 
	,drivername VARCHAR(100) 
	,driverlogin INTEGER 
	,driverpin INTEGER 
	,cardtype INTEGER 
	,drivercarddata BOOLEAN
	,speedaccuracydecimalplaces FLOAT
	,maxspeedaccuracydecimalplaces FLOAT
	,ecmflags INTEGER 
	,hasgforce BOOLEAN
	,cardidaslong INTEGER 
	,unitstatus INTEGER 
	,currentsetpointid INTEGER 
	,currentsetpointgroup INTEGER 
	,currentsetpointnumber INTEGER 
	,currentvehicleroutescheduleid INTEGER 
	,currentroutecheckpointindex INTEGER 
	,hdop FLOAT
	,numofsattelites INTEGER
	,coolanttemperature	FLOAT
	,oiltemperature	FLOAT
	,oilpressure	FLOAT
	,gear	INTEGER
	,maxrpm	INTEGER
	,brakeapplications	FLOAT
	,gforcefront	FLOAT
	,gforceback	FLOAT
	,gforceleftright	FLOAT
	,schoolzone	boolean
	,isprivate	boolean
	,roadtype	INTEGER
	 ,streetnumber	VARCHAR(10)
	,street	varchar(100)
	,suburb	varchar(100)
	,city	varchar(100)
	,state	varchar(100)
	,country	varchar(100)
	,postcode	INTEGER
	,speedlimit	FLOAT
  ,currentdatetime varchar(14)
	,PRIMARY KEY (customerreferencecode, instancename, fleetid, vehicleid, deviceid, serialnumber, devicetime, gpstime)
)
DISTSTYLE KEY
 DISTKEY (instancename)
 SORTKEY (
	fleetid
	, vehicleid
	);

	----------------------------------------
	
CREATE TABLE IF NOT EXISTS public.interface_process_control
(
	execution_date VARCHAR(10)
	,process_name VARCHAR(100)
	,unloaded_timestamp TIMESTAMP 
	,execution_date_with_timestamp TIMESTAMP 
)
