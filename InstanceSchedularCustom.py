#############################################################################################################################################################################
Description: This Script is used to get how long has it been Running/Stop (RDS,EC2) Instance by Aws Instance Schedular 
Logic : Get Information from CloudWatch logGroup of Instance Schedular, Apply  filter on log stream to pick Datetime, InstanceId,State and put that data in dynamoDb table
Author: Satyam Rai
#############################################################################################################################################################################



import json
import boto3
from datetime import datetime
from botocore.exceptions import ClientError



# List of Region where EC2/RDS are supposed to be run Instance Schedular
Regions = ['eu-north-1', 'ap-south-1', 'eu-west-3', 'eu-west-2', 'eu-west-1', 'ap-northeast-2', 'ap-northeast-1', 'sa-east-1', 'ca-central-1', 'ap-southeast-1', 'ap-southeast-2', 'eu-central-1', 'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2']


# Initializing  empty Lists 
State_Ec2 = []
InstanceId_Ec2 = []
DateTime_Ec2= []
State_Rds = []
InstanceId_Rds=[]
DateTime_Rds =[]

# Instantiate dynamo, client/Resource object
db = boto3.client('dynamodb')
client_log = boto3.client('logs')
dynamodb = boto3.resource('dynamodb')

# Instance Schedular log Group name
LogGroupName= "#########"
	       
def filterData(EC2_Stream,RDS_Stream):
    if(EC2_Stream):
        try:
            resp = client_log.filter_log_events(logGroupName=LogGroupName,
            logStreamNames=[EC2_Stream],
            #startTime=123,
            #endTime=123,
            filterPattern="True", 
            limit=10000)
            for data in resp['events']:
                state = data['message'].split()[7]
                Id_Info =  data['message'].split()[15]
                if Id_Info.find(",") != -1:
                    for id in Id_Info.split(","):
                        State_Ec2.append(state)
                        InstanceId_Ec2.append(id)
                        DateTime_Ec2.append(datetime.utcfromtimestamp(data['timestamp']/1000).strftime('%Y-%m-%d %H:%M:%S'))

                else:
                    State_Ec2.append(state)
                    InstanceId_Ec2.append(Id_Info)
                    DateTime_Ec2.append(datetime.utcfromtimestamp(data['timestamp']/1000).strftime('%Y-%m-%d %H:%M:%S'))
        except ClientError as e:
            print(e)
        CreateTable("EC2_Info")

    
    if(RDS_Stream):
        try:
            resp = client_log.filter_log_events(logGroupName=LogGroupName,
            logStreamNames=[RDS_Stream],
            #startTime=123,
            #endTime=123,
            filterPattern="True", 
            limit=10000)
            for data in resp['events']:
                InstanceId_Rds.append((data['message'].split())[-1])
                DateTime_Rds.append((data['message'].split())[0]+" "+ (data['message'].split())[2])
                State_Rds.append((data['message'].split())[7])
        except ClientError as e:
            print(e)
        CreateTable("RDS_Info")
    
def CreateTable(TableName):
    # Get an array of table names associated with the current account and endpoint.
    response = db.list_tables()

    if TableName in response['TableNames']:
        table_found = True
        Save(TableName)
    else:
        table_found = False
       

        # Create the DynamoDB table to store EC2/RDS Info
        table = dynamodb.create_table(
            TableName = TableName,
               AttributeDefinitions=[
                 {
                    "AttributeName": "InstanceId",
                    "AttributeType": "S"
                 }
                 ],
                KeySchema=[
                    {
                    "AttributeName": "InstanceId",
                    "KeyType": "HASH"
                    }
                  ],
                 ProvisionedThroughput={
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5
                    })
        # Wait until the table exists.
        table.meta.client.get_waiter('table_exists').wait(TableName=TableName)
        Save(TableName)
    return table_found
  
def Save(TableName):    
    dynamoTable =dynamodb.Table(TableName)
    if(TableName=="EC2_Info"):
        for i in range(0, len(InstanceId_Ec2)):
            dynamoTable.put_item(Item={'InstanceId':InstanceId_Ec2[i],'State':State_Ec2[i],'DateTime':DateTime_Ec2[i]})
            print("EC2putting")
          
    else:
        for i in range(0, len(InstanceId_Rds)):
            dynamoTable.put_item(Item={'InstanceId':InstanceId_Rds[i],'State':State_Rds[i],'DateTime':DateTime_Rds[i]})
            print("RdsPutting")

def lambda_handler(event, context):
    dt = datetime.utcnow()
    Account_Id = boto3.client('sts').get_caller_identity().get('Account')
    LOG_STREAM_EC2 = "Scheduler-ec2-{}-{}-{:0>4d}{:0>2d}{:0>2d}"
    LOG_STREAM_RDS = "Scheduler-rds-{}-{}-{:0>4d}{:0>2d}{:0>2d}"
    #log_stream_ec2  = Scheduler-ec2-371286728750-us-east-1-20200711
    #log_stream_rds  = Scheduler-rds-371286728750-us-east-1-20200711
    for Region in Regions:
        log_stream_ec2 = LOG_STREAM_EC2.format(Account_Id,Region, dt.year, dt.month, dt.day)
        log_stream_rds = LOG_STREAM_RDS.format(Account_Id,Region, dt.year, dt.month, dt.day)
        filterData(log_stream_ec2,log_stream_rds)
