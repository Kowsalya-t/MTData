import json, boto3, os, sys
from datetime import datetime

s3 = boto3.resource('s3')
sqs = boto3.resource('sqs')
sns = boto3.client('sns')
sqs_client = boto3.client('sqs')

Environment =  os.environ['environment']
snsTopic = os.environ['snsTopic']

def msg_brkr_read(event, context):
    
    try:
        receipt_handle      = event['Records'][0]['receiptHandle']
        Recs                = event['Records']
        No_of_recs_in_event = len(Recs)
        
        Idx_of_recs_in_event = 0
        
        while Idx_of_recs_in_event < No_of_recs_in_event:
            body         = event['Records'][Idx_of_recs_in_event]['body']
            bucketname   = json.loads(body)['Records'][0]['s3']['bucket']['name']
            env     = Environment 
            
            filename     = json.loads(body)['Records'][0]['s3']['object']['key']
            filename_new = filename.replace('+',' ').replace('%28','(').replace('%29',')').replace('%26','&')
            identity     = json.loads(body)['Records'][0]['s3']['object']['sequencer']
            filesize     = json.loads(body)['Records'][0]['s3']['object']['size']
                
            target_bucket_name='mtdata-datalake-'+env
                
            print("body :", json.dumps(body,default=str))
        
            #Defining the lists
            data = {}
            data['all_owner'] = []
            owner ={}
            owner['list'] = []
        
            now = datetime.now() 
        
            if filesize != 0:
                content_object = s3.Object(bucketname, filename_new)
                file_content = content_object.get()['Body'].read()
                json_content = json.loads(file_content)
                
                length =len(json_content)
                list_no = 0
                uniq_owner_set = set()       
                
                while list_no < length:
                    #Data from Message control block
                    Msg_Broad_type = ['GEN_G_FORCES_SUSTAINED_START','GEN_G_FORCES_SUSTAINED_END','TAMPER','GEN_NO_GPS_ANT_OPEN','GEN_NO_GPS_ANT_SHORT','ALRM_SUSPECT_GPS',
                                        'ALRM_IGNITION_DISCONNECT','ALRM_ENGINE_DATA_MISSING','ALRM_SUSPEC_ENGINE_DATA','ALRM_G_FORCE_MISSING','ALRM_G_FORCE_OUT_OF_CALIBRATION',
                                        'ALRM_CONCETE_SENSOR_ALERT','ALRM_MDT_NOT_LOGGED_IN','ALRM_MDT_NOT_BEING_USED','ALRM_VEHICLE_GPS_SPEED_DIFF','GEN_IF_CONFIG_RESTART',
                                        'GEN_ZERO_BYTES_RECEIVED_RESTART','STATUS_VEHICLE_BREAK_START','STATUS_VEHICLE_BREAK_END','GEN_MDT_STATUS_REPORT','PENDENT_ALARM_TEST',
                                        'PENDENT_ALARM','PENDENT_ALARM_CLEARED','PENDENT_ALARM_ACKD_BY_USER','PENDENT_ALARM_NOT_ACKD_BY_USER','SHELL_REQUEST_REBOOT','SAT_PING',
                                        'EXTENDED_IO_BELOW_THRESHOLD','EXTENDED_IO_ABOVE_THRESHOLD','STATUS_LOGIN','STATUS_LOGOUT','BARREL_RPM_UP_TO_SPEED','BARREL_RPM_STOPPED',
                                        'BARREL_RPM_REVERSED','BARREL_RPM_FORWARD','BARREL_MIX_STARTED','BARREL_NOT_MIXED','BARREL_LEFT_LOADER','BARREL_ABOVE_ONSITE_SPEED',
                                        'BARREL_LOADING_STARTED','BARREL_CONCRETE_AGE_MINS','BARREL_CONCRETE_AGE_ROTATIONS','OVERSPEED_ZONE_1_START','OVERSPEED_ZONE_2_START',
                                        'OVERSPEED_ZONE_3_START','OVERSPEED_ZONE_1_END','OVERSPEED_ZONE_2_END','OVERSPEED_ZONE_3_END','ANGEL_GEAR_ALERT_START','ANGEL_GEAR_ALERT_END',
                                        'FATIGUE_REPORT_IGNON','FATIGUE_REPORT_24','GEN_NOGPS','GEN_OVERSPEED','GEN_EXCESSIVE_IDLE','STATUS_REPORT','STATUS_POS_REPORT','STATUS_IGNITION_ON',
                                        'STATUS_TRAILER_HITCH','STATUS_TRAILER_DEHITCH','STATUS_DATA_USAGE_ALERT','STATUS_DATA_USAGE_ALERT_SATELLITE','GPS_HISTORY','GEN_ECM_OVERSPEED','LOW_POWER_MODE',
                                        'DRIVER_PERSONAL_KMS'
                                     ] #This will be extended as Michael decodes more data
                    
                    if 'lst' in json_content[list_no]['ServiceBrokerMessage']['messageControl']['publishTopic'] and json_content[list_no]['ServiceBrokerMessage']['messageDetail']['Header']['MsgType'] in Msg_Broad_type :
                        Msg_Ctrl_Data = ['instanceName','fleetId','vehicleId','deviceID','serialNumber','deviceType']
                        for item in Msg_Ctrl_Data:
                            globals()['%s' % item] = json_content[list_no]['ServiceBrokerMessage']['messageControl'][item]

                        Srvr_Date = ['arrivalTimestamp']	
                        for item in Srvr_Date:
                            globals()['%s' % item] = json_content[list_no]['ServiceBrokerMessage']['messagePayload'][item]

                        Header_Info = ['MsgTypeID','MsgType']     
                        for item in Header_Info:
                            globals()['%s' % item] = json_content[list_no]['ServiceBrokerMessage']['messageDetail']['Header'][item]
                        
                        if 'customerId' not in json_content[list_no]['ServiceBrokerMessage']['messageControl']:
                            ownerId='null'
                            CustomerReferenceCode = 'null'
                        else:
                            ownerId = json_content[list_no]['ServiceBrokerMessage']['messageControl']['customerId']
                            CustomerReferenceCode = json_content[list_no]['ServiceBrokerMessage']['messageControl']['customeReference']
                            if ownerId is None:
                                ownerId='null'
                            if CustomerReferenceCode is None:
                                CustomerReferenceCode ='null'    
                        
                        Message_type = ['ExtendedVariableDetails','ECMAdvanced','EngineData','ExtraDetails','TrailerTrack','EngineSummaryData','ExtraDetails','HasRoadTypeData'] 
                        
                        for item in Message_type:
                            if json_content[list_no]['ServiceBrokerMessage']['messageDetail']['Header'][item] is True:
                                if item == 'ExtraDetails':
                                    item = 'TransportExtraDetails'
                                if item ==  'HasRoadTypeData'  :
                                    item = 'RoadTypeData'
                                
                                globals()['key_value_pair_%s' % item] = []
                                globals()['key_value_pair_dict_%s' % item] = {}
                                dict = json_content[list_no]['ServiceBrokerMessage']['messageDetail']['Data'][item]
                                for key, value in dict.items():
                                    globals()['key_value_pair_dict_%s' % item][key] = value
                                globals()['key_value_pair_%s' % item].append(globals()['key_value_pair_dict_%s' % item])    #creating list of dictionary
                                
                        Other_Message_type = ['CurrentClock' , 'FixClock','Fix','Status','Distance']    
                        
                        for item in Other_Message_type:
                            globals()['key_value_pair_%s' % item] = []
                            globals()['key_value_pair_dict_%s' % item] = {}
                            dict = json_content[list_no]['ServiceBrokerMessage']['messageDetail']['Data'][item]
                            for key, value in dict.items():
                                globals()['key_value_pair_dict_%s' % item][key] = value
                            globals()['key_value_pair_%s' % item].append(globals()['key_value_pair_dict_%s' % item])    #creating list of dictionary

                        
                        mydict = {}
                        for values in Msg_Ctrl_Data + Srvr_Date + Header_Info:
                            try:
                                mydict[values] = globals()['%s' % values]
                            except:
                                print("No data for "+values+" in message bus")
                                
                        Revised_Message_Type =  ['ExtendedVariableDetails','ECMAdvanced','EngineData','ExtraDetails','TrailerTrack','EngineSummaryData','TransportExtraDetails','RoadTypeData'] 
                        
                        for values in Other_Message_type + Revised_Message_Type  :
                            try:
                                mydict[values] = globals()['key_value_pair_%s' % values]
                            except:
                                print("No data for "+values+" in message bus")
                        
                        print("mydict :" , mydict)        
                        mydict['CustomerReferenceCode'] = CustomerReferenceCode
                        data['all_owner'].append(mydict)
                        owner['list'].append(CustomerReferenceCode)
                        
                        #Creating a set to get unique values in the owner list so as to partition on the owner_name
                        uniq_owner_set = set(owner['list'])
                        print("uniq_owner_set :", uniq_owner_set)
                        
                    list_no = list_no+1
                
                unique_owner_list = (list(uniq_owner_set)) # null , 4 owner id etc
                
                len_ownerid = len(unique_owner_list)
                owner_list_id = 0
                
                #This loop is to initialize the list
                while owner_list_id < len_ownerid:
                    data[str(unique_owner_list[owner_list_id])] = []
                    owner_list_id = owner_list_id + 1
                
                len_allowner = len(data['all_owner']  )
                owner_list_id = 0
            
                while owner_list_id < len_allowner:
                    
                    mydict = {}
                    mydict['CustomerReferenceCode'] = data['all_owner'][owner_list_id]['CustomerReferenceCode']
                    for values in   Msg_Ctrl_Data + Srvr_Date + Header_Info + Other_Message_type + Revised_Message_Type :
                        try:
                            mydict[values] = data['all_owner'][owner_list_id][values]
                        except:
                            print(" In loop No data for "+values+" in message bus")
                           
                    data[str(data['all_owner'][owner_list_id]['CustomerReferenceCode'])].append(mydict)
                    
                    owner_list_id = owner_list_id+1
            
                owner_id = 0
                while owner_id < len_ownerid:
                    oReference = str(unique_owner_list[owner_id])
                    oReference_new = str(unique_owner_list[owner_id]).replace(' ','')
                    print(str(unique_owner_list[owner_id]))
                    print("oReference : ", oReference)
                    fn  = '/tmp/trip_data_'+oReference+'_'+identity+'.txt'
                    
                    with open(fn, 'a+') as f:
                        time        = now.strftime("%y_%m_%d_%H%M%S")
                        s3_key_name = 'datalake_lnd/cust_ref='+oReference_new+'/trip_data_'+oReference_new+'_'+time+'_'+identity+'.json'
                        s3object    = s3.Object(target_bucket_name,s3_key_name)
                        s3object.put(
                            Body=(bytes(json.dumps(data[oReference]).encode('UTF-8')))
                        )
                        
                    owner_id = owner_id + 1
                    print(s3_key_name)
                
            Idx_of_recs_in_event = Idx_of_recs_in_event + 1    
        
        message  = sqs.Message(sqs_client.get_queue_url(QueueName = 'msg_brkr_q_'+Environment)['QueueUrl'],receipt_handle)
        response = message.delete()  
    except:    
        try:
            message = "File: " + filename_new + '\n\nError message: ' + str(sys.exc_info())

            print("snsTopic ",snsTopic)

            TargetArn_str = snsTopic
            
            response = sns.publish(
                TargetArn=TargetArn_str,
                Message=message,
                Subject=context.function_name,
                MessageStructure='string'
            )
            
        except:    
            print("Failed :-( :", sys.exc_info())
    
    return("Success")