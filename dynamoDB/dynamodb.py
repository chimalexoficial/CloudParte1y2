import boto3
from boto3.dynamodb.conditions import Key
import json
from .configuration import Config

# If you are using aws educate account you must update your credentials in
# ~/.aws/credentials every 3 hours.
class Dynamodb():
    def __init__(self, path_conf):
        self.conf = Config(path_conf)
        # Init resource with region in Conf class.
        self.resource = boto3.resource(
            'dynamodb',
            region_name=self.conf.get_region()
        )

        try:
            self.resource.create_table(
                AttributeDefinitions=[
                    {
                        'AttributeName': 'keyword',
                        'AttributeType': 'S'
                    }
                ],
                TableName='images',
                KeySchema=[
                    {
                        'AttributeName': 'keyword',
                        'KeyType': 'HASH'
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
        except Exception as e:
            print("Exception ocurred creating images table: {}".format(e.__class__.__name__))
        else:
            print('Images table created')

        try:
            self.resource.create_table(
                AttributeDefinitions=[
                    {
                        'AttributeName': 'keyword',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'inx',
                        'AttributeType': 'N'
                    }
                ],
                TableName='labels',
                KeySchema=[
                    {
                        'AttributeName': 'keyword',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'inx',
                        'KeyType': 'RANGE'
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
        except Exception as e:
            print("Exception ocurred creating labels table: {}".format(e.__class__.__name__))
        else:
            print('Labels table created')

        self.table_images = self.resource.Table('images')
        self.table_labels = self.resource.Table('labels')

    def get_conf(self):
        return self.conf

    def put_image(self, keyword, url):
        try:
            self.table_images.put_item(
                Item={
                    'keyword': keyword,
                    'url': url
                }
            )
        except Exception as e:
            print("Exception ocurred adding item to images table: {}".format(e.__class__.__name__))
        else:
            print('Item added')

    def put_label(self, keyword, inx, category):
        print("put_label")
        try:
            self.table_labels.put_item(
                Item={
                    'keyword': keyword,
                    'inx': int(inx),
                    'category': category
                }
            )
        except Exception as e:
            print("Exception ocurred adding item to label table: {}".format(e.__class__.__name__))
        else:
            print('Item added')
