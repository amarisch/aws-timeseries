import os
import botos
import datetime
import csv
import codecs
from csv import reader

# Usage scenario: sensor data has already been uploaded to S3 and need to be migrated to DynamoDB
# The CSV data in S3 contains the format: id, timestamp, observable, value
# the CSV file name contains date information, and should be deleted after injection
#

region = os.environ.get('AWS_DEFAULT_REGION', 'us-west-2')
dynamodb = boto3.client('dynamodb', region_name=region)
s3 = boto3.client('s3')

class ImportData(object):

    def __init__(self, table_prefix, bucketName):
        self.table_prefix = table_prefix
        self.bucket = s3.Bucket(bucketName)
        self.bucketName = bucketName

    def import_data(self):
        for obj in self.bucket.objects.all():
            current = obj.last_modified.date
            tableName = "%s_%s" % (self.table_prefix, self._format_date(current))
            try:
                content = obj.get()['Body']
            except:
                print("S3 Object could not be opened.")
            try:
                self.table = dynamodb.Table(tableName)
            except:
                print("Error loading DynamoDB table. Check if table was created correctly and environment variable.")
    
            with open(content) as csv_file:
                tokens = reader(csv_file, delimiter=',')
                header = next(tokens) # field names
                header = header[2:]
                for token in tokens:
                    timestamp = token[1]
                    userId = token[0]
                    with self.table.batch_write() as batch:
                        for i, val in enumerate(token[2:]):
                            if val:
                                batch.put_item(Item={"userId_observable": "{}_{}".format(userId, header[i]), \
                                                     "time": timestamp, "value": val})  
    
            # delete file from S3 bucket
            s3.delete_object(Bucket=self.bucketName, Key=obj.key)

        return {
            'statusCode': 200,
            'body': json.dumps('Uploaded to DynamoDB')
        }


    @staticmethod
    def _format_date(d):          
        return d.strftime("%Y-%m-%d")
