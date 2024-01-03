import datetime
from dateutil.tz import tzlocal

LIST_DATABASES ={
    'DatabaseList': [
        {
            'CatalogId': '672373165745',
            'CreateTableDefaultPermissions': [{'Permissions': ['ALL'],
                                            'Principal': {'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'}}],
            'CreateTime': datetime.datetime(2023, 12, 18, 22, 28, 55, tzinfo=tzlocal()),
            'Description': 'This is a database containing customers information',
            'LocationUri': 's3://aws-bucket-no-1/data/customer_database/',
            'Name': 'customer_database'
        },
        {
            'CatalogId': '123456789012',
            'CreateTableDefaultPermissions': [{'Permissions': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
                                            'Principal': {'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'}}],
            'CreateTime': datetime.datetime(2023, 11, 15, 18, 45, 30, tzinfo=tzlocal()),
            'Description': 'Database for product inventory',
            'LocationUri': 's3://aws-bucket-no-2/data/product_database/',
            'Name': 'product_database'
        },
        {
            'CatalogId': '987654321098',
            'CreateTableDefaultPermissions': [{'Permissions': ['ALL'],
                                            'Principal': {'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'}}],
            'CreateTime': datetime.datetime(2023, 10, 5, 12, 10, 45, tzinfo=tzlocal()),
            'Description': 'Human resources database',
            'LocationUri': 's3://aws-bucket-no-3/data/hr_database/',
            'Name': 'hr_database'
        }
    ]
}

LIST_TABLES = {
    'TableList': [
        {
            'CatalogId': '672373165745',
            'CreateTime': datetime.datetime(2023, 12, 18, 22, 58, 52, tzinfo=tzlocal()),
            'DatabaseName': 'customer_database',
            'IsRegisteredWithLakeFormation': False,
            'LastAccessTime': datetime.datetime(2023, 12, 18, 22, 58, 53, tzinfo=tzlocal()),
            'Name': 'customer_csv',
            'Owner': 'owner',
            'Parameters': {
                'CrawlerSchemaDeserializerVersion': '1.0',
                'CrawlerSchemaSerializerVersion': '1.0',
                'UPDATED_BY_CRAWLER': 'crawler_customer_csv',
                'areColumnsQuoted': 'false',
                'averageRecordSize': '197',
                'classification': 'csv',
                'columnsOrdered': 'true',
                'compressionType': 'none',
                'delimiter': ',',
                'objectCount': '1',
                'partition_filtering.enabled': 'true',
                'recordCount': '998',
                'sizeKey': '196676',
                'skip.header.line.count': '1',
                'typeOfData': 'file'
            },
            'PartitionKeys': [{'Name': 'dataload', 'Type': 'string'}],
            'Retention': 0,
            'StorageDescriptor': {
                'BucketColumns': [],
                'Columns': [
                    {'Name': 'customerid', 'Type': 'bigint'},
                    {'Name': 'namestyle', 'Type': 'boolean'},
                    {'Name': 'title', 'Type': 'string'}
                ],
                'Compressed': False,
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'Location': 's3://aws-bucket-no-1/data/customer_database/customer_csv/',
            },
            'TableType': 'EXTERNAL_TABLE',
            'UpdateTime': datetime.datetime(2023, 12, 18, 22, 58, 53, tzinfo=tzlocal()),
            'VersionId': '1'
        }
    ]
}

LIST_CRAWLERS = {
    'Crawlers':   [
        {
            'Classifiers': ['classifier1', 'classifier2'],
            'Configuration': '{"Version":1.1,"CreatePartitionIndex":false}',
            'CrawlElapsedTime': 120,
            'CreationTime': datetime.datetime(2023, 12, 19, 10, 15, 30, tzinfo=tzlocal()),
            'DatabaseName': 'sales_database',
            'LakeFormationConfiguration': {'AccountId': '123456789012', 'UseLakeFormationCredentials': True},
            'LastCrawl': {
                'LogGroup': '/aws-glue/crawlers',
                'LogStream': 'crawler_sales_data',
                'MessagePrefix': '98765432-1234-5678-abcd-1234567890ab',
                'StartTime': datetime.datetime(2023, 12, 19, 10, 17, 45, tzinfo=tzlocal()),
                'Status': 'SUCCEEDED'
            },
            'LastUpdated': datetime.datetime(2023, 12, 19, 10, 15, 30, tzinfo=tzlocal()),
            'LineageConfiguration': {'CrawlerLineageSettings': 'ENABLE'},
            'Name': 'crawler_sales_data',
            'RecrawlPolicy': {'RecrawlBehavior': 'CRAWL_NEW_AND_CHANGED'},
            'Role': 'sales-glue-user',
            'SchemaChangePolicy': {'DeleteBehavior': 'LOG', 'UpdateBehavior': 'UPDATE_IN_DATABASE'},
            'State': 'READY',
            'Targets': {
                'CatalogTargets': [],
                'DeltaTargets': [],
                'DynamoDBTargets': [],
                'HudiTargets': [],
                'IcebergTargets': [],
                'JdbcTargets': [],
                'MongoDBTargets': [],
                'S3Targets': [
                    {
                        'Exclusions': ['path/to/exclude1', 'path/to/exclude2'],
                        'Path': 's3://aws-bucket-no-4/data/sales_database/sales_data/'
                    }
                ]
            },
            'Version': 2
        }
    ]
}

LIST_CONNECTIONS = {
    'ConnectionList': [
        {
            'ConnectionProperties': {
            'S3_CONNECTION_URL': 's3://aws-bucket-no-2/data/customer_data',
            'S3_REGION': 'us-east-1',
            'AWS_ACCESS_KEY_ID': 'your_access_key_id',
            'AWS_SECRET_ACCESS_KEY': 'your_secret_access_key'
            },
            'ConnectionType': 'S3',
            'CreationTime': datetime.datetime(2024, 1, 2, 15, 30, 45, tzinfo=tzlocal()),
            'Description': 'Sample S3 Connection',
            'LastUpdatedBy': 'admin',
            'LastUpdatedTime': datetime.datetime(2024, 1, 2, 15, 30, 45, tzinfo=tzlocal()),
            'Name': 'S3 Connection'
        },
        {
            'ConnectionProperties': {
            'REDSHIFT_CONNECTION_URL': 'redshift://your-redshift-cluster-url',
            'USERNAME': 'your_redshift_username',
            'PASSWORD': 'your_redshift_password'
            },
            'ConnectionType': 'REDSHIFT',
            'CreationTime': datetime.datetime(2024, 1, 2, 16, 45, 20, tzinfo=tzlocal()),
            'Description': 'Sample Redshift Connection',
            'LastUpdatedBy': 'admin',
            'LastUpdatedTime': datetime.datetime(2024, 1, 2, 16, 45, 20, tzinfo=tzlocal()),
            'Name': 'Redshift Connection'
        }
    ]
}

LIST_JOBS = {
    'Jobs': [
        {
            'AllocatedCapacity': 8,
            'Command': {
                'Name': 'glueetl',
                'PythonVersion': '3',
                'ScriptLocation': 's3://aws-bucket-no-1/scripts/data-processing-job.py'
            },
            'CreatedOn': datetime.datetime(2023, 12, 21, 10, 45, 22, 500000, tzinfo=tzlocal()),
            'DefaultArguments': {
                '--TempDir': 's3://aws-bucket-no-1/temporary-directory/',
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-glue-datacatalog': 'true',
                '--enable-job-insights': 'false',
                '--enable-metrics': 'true',
                '--enable-observability-metrics': 'true',
                '--enable-spark-ui': 'true',
                '--job-bookmark-option': 'job-bookmark-disable',
                '--job-language': 'python',
                '--spark-event-logs-path': 's3://aws-glue-assets-672373165745-us-east-1/sparkHistoryLogs/'
            },
            'Description': 'Data processing job for analytics',
            'ExecutionClass': 'STANDARD',
            'ExecutionProperty': {'MaxConcurrentRuns': 1},
            'GlueVersion': '4.1',
            'LastModifiedOn': datetime.datetime(2023, 12, 21, 10, 50, 38, 900000, tzinfo=tzlocal()),
            'MaxCapacity': 8.0,
            'MaxRetries': 2,
            'Name': 'data-processing-job',
            'NumberOfWorkers': 8,
            'Role': 'arn:aws:iam::672373165745:role/default-glue-user',
            'Timeout': 2400,
            'WorkerType': 'Standard'
        }
    ]
}

LIST_INTERACTIVE_SESSIONS = {
    'Sessions': [
        {
            'Command': {'Name': 'gluescript', 'PythonVersion': '3.7'},
            'CompletedOn': datetime.datetime(2023, 11, 15, 14, 30, 10, 500000, tzinfo=tzlocal()),
            'CreatedOn': datetime.datetime(2023, 11, 15, 14, 0, 0, 0, tzinfo=tzlocal()),
            'DPUSeconds': 2450.78,
            'ExecutionTime': 1225.39,
            'GlueVersion': '3.0',
            'Id': 'glue-interactive-session-1',
            'MaxCapacity': 1.5,
            'Progress': 50.0,
            'Role': 'arn:aws:iam::123456789012:role/glue-user-role',
            'Status': 'SUCCEEDED'
        },
        {
            'Command': {'Name': 'pyspark', 'PythonVersion': '3.6'},
            'CompletedOn': datetime.datetime(2023, 11, 15, 16, 45, 30, 800000, tzinfo=tzlocal()),
            'CreatedOn': datetime.datetime(2023, 11, 15, 16, 15, 0, 0, tzinfo=tzlocal()),
            'DPUSeconds': 3189.23,
            'ExecutionTime': 1594.62,
            'GlueVersion': '4.0',
            'Id': 'glue-interactive-session-2',
            'MaxCapacity': 2.0,
            'Progress': 75.0,
            'Role': 'arn:aws:iam::123456789012:role/glue-user-role',
            'Status': 'SUCCEEDED'
        }
    ]
}



