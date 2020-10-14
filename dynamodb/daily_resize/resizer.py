import os
import boto3
import datetime

region = os.environ.get('AWS_DEFAULT_REGION', 'us-west-2')
dynamodb = boto3.client('dynamodb', region_name=region)


#    TableName: timeseries_2019-05
#    +---------------+---------------+--------+---------------+
#    | Partition Key |   Sort Key    | User Id|     Meta      |
#    +---------------+---------------+--------+---------------+
#    | 10_heartRate  | 1557807567857 |     10 | { other data} |
#    | ..            |            .. |     .. | ..            |
#    | ..            |            .. |     .. | ..            |
#    +---------------+---------------+--------+---------------+




class DailyResize(object):
    
    FIRST_DAY_RCU, FIRST_DAY_WCU = 300, 1000
    SECOND_DAY_RCU, SECOND_DAY_WCU = 100, 1
    THIRD_DAY_RCU, THIRD_DAY_WCU = 1, 1

    def __init__(self, table_prefix):
        self.table_prefix = table_prefix
    
    def create_new(self):
        # create new table (300 RCU, 1000 WCU)
        today = datetime.date.today()
        new_table_name = "%s_%s" % (self.table_prefix, self._format_date(today))
        dynamodb.create_table(
            TableName=new_table_name,
            KeySchema=[       
                { 'AttributeName': "userId_observable", 'KeyType': "HASH"},  # Partition key
                { 'AttributeName': "time", 'KeyType': "RANGE" } # Sort key
            ],
            AttributeDefinitions=[       
                { 'AttributeName': "userId_observable", 'AttributeType': "S" },
                { 'AttributeName': "time", 'AttributeType': "N" }
            ],
            ProvisionedThroughput={       
                'ReadCapacityUnits': self.FIRST_DAY_RCU, 
                'WriteCapacityUnits': self.FIRST_DAY_WCU,
            },
        )
    
        print("Table created with name '%s'" % new_table_name)
        return new_table_name
    
    
    def resize_old(self):
        # update yesterday's table (100 RCU, 1 WCU)
        yesterday = datetime.date.today() - datetime.timedelta(1)
        old_table_name = "%s_%s" % (self.table_prefix, self._format_date(yesterday))
        self._update_table(old_table_name, self.SECOND_DAY_RCU, self.SECOND_DAY_WCU)
    
        # update the day before yesterday's table (1 RCU, 1 WCU)
        the_day_before_yesterday = datetime.date.today() - datetime.timedelta(2)
        very_old_table_name = "%s_%s" % (self.table_prefix, self._format_date(the_day_before_yesterday))
        self._update_table(very_old_table_name, self.THIRD_DAY_RCU, self.THIRD_DAY_WCU)
        
        return "OK"
        
    
    def _update_table(self, table_name, RCU, WCU):
        """ Update RCU/WCU of the given table (if exists) """
        print("Updating table with name '%s'" % table_name)
        try:
            dynamodb.update_table(
                TableName=table_name,
                ProvisionedThroughput={
                    'ReadCapacityUnits': RCU,
                    'WriteCapacityUnits': WCU,
                },
            )
        except dynamodb.exceptions.ResourceNotFoundException as ex:
            print("DynamoDB Table %s not found" % table_name)
    
    
    @staticmethod
    def _format_date(d):
        return d.strftime("%Y-%m-%d")
