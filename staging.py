import csv, json, sys, boto3 ,os
from datetime import datetime

s3 = boto3.resource('s3')
sqs = boto3.resource('sqs')
sqs_client = boto3.client('sqs')
sns = boto3.client('sns')

Environment =  os.environ['environment']
snsTopic = os.environ['snsTopic']

def assignment(data,list_item,index,list_name):
    for item in list_item:
        try:
            globals()['%s' % item] = data[index][item]
        except:
            try:
                globals()['%s' % item] = data[index][list_name][0][item]
            except:    
                print(str(item)," not in Landing :", sys.exc_info())
    return

def convert_date(date_col):
    if len(date_col) == 2:
        return date_col
    else:
        return '0'+ date_col
    
def trnsfrm_stg_lake_msg_broker(event, context):
    try:
        print("event :",json.dumps(event))
        
        receipt_handle = event['Records'][0]['receiptHandle']
        Recs = event['Records']
        No_of_recs_in_event = len(Recs)
        
        Idx_of_recs_in_event = 0
        
        while Idx_of_recs_in_event < No_of_recs_in_event:
            body = event['Records'][Idx_of_recs_in_event]['body']
            src_bucketname = json.loads(body)['Records'][0]['s3']['bucket']['name']
            try:
                env = src_bucketname.split("-",2)[2] 
            except:
                env = ''
            
            filename = json.loads(body)['Records'][0]['s3']['object']['key']
            filename_new = filename.replace('%3D','=').replace('%26','&')
            identity = filename_new.split('/')[2].split('_')[7].split('.')[0]
            
            if env == '':
                target_bucket_name='mtdata-datalake'
            else:    
                target_bucket_name='mtdata-datalake-'+env
            
            content_object = s3.Object(src_bucketname,filename_new)
            file_content = content_object.get()['Body'].read()
            data = json.loads(file_content)
            
            length = len(data)
            index = 0
            csv_data = {}
            csv_data['list'] = []
            
            Msg_Ctrl_Data = ['CustomerReferenceCode','instanceName','fleetId','vehicleId','deviceID','deviceType','serialNumber']
            Date_Col = ['deviceTime','GPSTime','arrivalTimestamp']
            Fix_Data =  ['Latitude','Longitude','Flags','Direction','Speed']
            Distance_Data = ['MaxSpeed']
            Status_Data = ['InputStatus','StatusFlag','OutputStatus','ButtonStatus','LightStatus','SpareStatus']
            ECMAdvanced_Data = ['Satellites_Altitude','Satellites_NoInView','Satellites_HDOP','SpeedAccuracyDP','GPSOdometer',
                                        'DriverName','DriverLogin','DriverPin','CardType','DriverCardData',
                                        'SpeedAccuracyDecimalPlaces','MaxSpeedAccuracyDecimalPlaces','ECMFlags','HasGForce','CardIDasLong' ]
            #SPNIdList =['Speed','MaxSpeed']                                    
            ExtendedVariableDetails_Data = ['UnitStatus','CurrentSetPointID','CurrentSetPointGroup','CurrentSetPointNumber','CurrentVehicleRouteScheduleID','CurrentRouteCheckPointIndex','HDOP','NumOfSattelites'] 
            Control_Id = ['ownerId','fleetId','vehicleId']
            EngineData = ['CoolantTemperature','OilTemperature','OilPressure','Gear','MaxRPM', 'BrakeApplications', 'GForceFront', 'GForceBack', 'GForceLeftRight']
            RoadTypeData = ['SchoolZone','IsPrivate','RoadType','StreetNumber','Street','Suburb','City','State','Country','PostCode','SpeedLimit']
            
            
            while index < length:
                if data[index]['vehicleId'] is not None and data[index]['fleetId'] is not None:
                    try:
                        Tot_No_of_Advanced_Items = data[index]['ECMAdvanced'][0]['Count']
                    except:
                        Tot_No_of_Advanced_Items = 0

                    assignment(data,Msg_Ctrl_Data,index,'Msg_Ctrl_Data')
                    
                    global deviceTime , GPSTime , arrivalTimestamp
                    for item in Date_Col:
                        try:
                            if item == 'deviceTime':
                                if 'DecodeStatus' not in data[index]['CurrentClock'][0]:
                                    deviceTime = str(data[index]['CurrentClock'][0]['Year'])+ '-' + convert_date(str(data[index]['CurrentClock'][0]['Month'])) + '-' + convert_date(str(data[index]['CurrentClock'][0]['Day'])) + ' ' + convert_date(str(data[index]['CurrentClock'][0]['Hour'])) + ':' + convert_date(str(data[index]['CurrentClock'][0]['Minute'])) + ':' + convert_date(str(data[index]['CurrentClock'][0]['Second']))
                                else:
                                    deviceTime = ''
                                
                            if item == 'GPSTime':
                                if 'DecodeStatus' not in data[index]['FixClock'][0]:
                                    GPSTime = str(data[index]['FixClock'][0]['Year'])+ '-' + convert_date(str(data[index]['FixClock'][0]['Month'])) + '-' + convert_date(str(data[index]['FixClock'][0]['Day'])) + ' ' + convert_date(str(data[index]['FixClock'][0]['Hour'])) + ':' + convert_date(str(data[index]['FixClock'][0]['Minute'])) + ':' + convert_date(str(data[index]['FixClock'][0]['Second']))
                                else:
                                    GPSTime = ''
                                    
                            if item == 'arrivalTimestamp':
                                arrivalTimestamp = str(data[index]['arrivalTimestamp']).replace("T"," ").split('+',1)[0]
                        except:
                            print(item+" not in Landing :"+ sys.exc_info() )
                    
                    assignment(data,Fix_Data,index,'Fix')
                    
                    assignment(data,Distance_Data,index,'Distance')
                            
                    assignment(data,Status_Data,index,'Status')
                    
                    assignment(data,ECMAdvanced_Data,index,'ECMAdvanced')
                    
                    ########Commenting the below as Speed and MaxSpeed is calculated from message bus###########
                    #counter = 0
                    #global Speed, MaxSpeed
                    #Initialize all atrributes for the event.
                    #Speed = ''
                    #MaxSpeed = ''
                    
                    #while counter < Tot_No_of_Advanced_Items:
                    #    try:
                    #        spn_id_value = data[index]['ECMAdvanced'][0]['ECMAdvancedItems'][counter]['SPN_ID']
                    #        source_type_value = data[index]['ECMAdvanced'][0]['ECMAdvancedItems'][counter]['SourceType']
                    #        Raw_Value = data[index]['ECMAdvanced'][0]['ECMAdvancedItems'][counter]['RawValue']
                    #        if spn_id_value == 5 and source_type_value == 6:
                    #            Speed = Raw_Value / 185.2
                                
                    #        if spn_id_value == 6 and source_type_value == 6:
                    #            MaxSpeed = Raw_Value / 185.2    
                    #    except:
                    #        print("Error in ECMAdvancedItems_Data :", sys.exc_info())
                        
                    #    counter = counter +1        
                    
                    assignment(data,ExtendedVariableDetails_Data,index,'ExtendedVariableDetails') 
                    
                    assignment(data,EngineData,index,'EngineData')
                    
                    assignment(data,RoadTypeData,index,'RoadTypeData')
                        
                    mydict = {}
                    
                    for values in Msg_Ctrl_Data + Date_Col + Fix_Data + Distance_Data + Status_Data + ECMAdvanced_Data + ExtendedVariableDetails_Data + EngineData + RoadTypeData:
                        try:
                            mydict[values] = globals()['%s' % values]
                        except:
                            print(values+" not added to dictionary :" ,  sys.exc_info()) 
                            
                    csv_data['list'].append(mydict)
                    
                else:
                    assignment(data,Control_Id,index,'Control_Id')
                    
                    
                index = index + 1            
                
            fileOutput = '/tmp/test_'+identity+'.csv'
            
            csv_columns = Msg_Ctrl_Data + Date_Col  + Fix_Data + Distance_Data + Status_Data + ECMAdvanced_Data + ExtendedVariableDetails_Data + EngineData + RoadTypeData
            
            try:
                with open(fileOutput, 'w') as csvfile:
                    writer = csv.DictWriter(csvfile,fieldnames=csv_columns)
                    writer.writeheader()
                    for formatted_data in csv_data['list']:
                        writer.writerow(formatted_data)
            except IOError:
                print("I/O error :", sys.exc_info()) 
            
            now  = datetime.now()    
            time = now.strftime("%y_%m_%d_%H%M%S") 
            date = now.strftime("%Y%m%d")

            formatted_custRefCode = str(globals()['CustomerReferenceCode']).replace('+','').replace(' ','').replace('%26','&')
            
            if len(csv_data['list']) > 0:
                s3.meta.client.upload_file('/tmp/test_'+identity+'.csv', src_bucketname, 'datalake_stg/cust_ref='+formatted_custRefCode+'/fleetId='+str(globals()['fleetId'])+'/vehicleId='+str(globals()['vehicleId'])+'/'+date+'/trnsfrmd_trip_data_'+time+'_'+identity+'.csv')
            
            Idx_of_recs_in_event = Idx_of_recs_in_event + 1
            
        message  = sqs.Message(sqs_client.get_queue_url(QueueName = 'mtdata-datalake-landing-'+Environment)['QueueUrl'],receipt_handle)
        response = message.delete() 

    except:    
        try:
            message = "File: " + filename_new + '\n\nError message: ' + str(sys.exc_info())
            
            TargetArn_str = snsTopic
            
            response = sns.publish(
                TargetArn=TargetArn_str,
                Message=message,
                Subject=context.function_name,
                MessageStructure='string'
            )
            
        except:    
            print("Failed :-( :", sys.exc_info())
        
    return("success")