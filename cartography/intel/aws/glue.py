import logging
from pprint import pprint
from typing import Any
from typing import Dict
from typing import List

import boto3
import neo4j

from cartography.stats import get_stats_client
from cartography.util import run_cleanup_job
from cartography.util import timeit


logger = logging.getLogger(__name__)
stat_handler = get_stats_client(__name__)

@timeit
def list_all_databases(boto3_session: boto3.session.Session, region: str) -> List[Dict[str, Any]]:
    client = boto3_session.client('glue', region_name=region)
    paginator =client.get_paginator('get_databases')
    databases: List[Dict[str, Any]] = []

    for page in paginator.paginate():
        databases.extend(page['DatabaseList'])

    return databases

@timeit
def list_all_crawlers(boto3_session: boto3.session.Session, region: str) -> List[Dict[str, Any]]:
    client = boto3_session.client('glue', region_name=region)
    paginator = client.get_paginator('get_crawlers')
    crawlers: List[Dict[str, Any]] = []

    for page in paginator.paginate():
        crawlers.extend(page['Crawlers'])

    return crawlers

@timeit
def list_all_jobs(boto3_session: boto3.session.Session, region: str) -> List[Dict[str, Any]]:
    client = boto3_session.client('glue', region_name=region)
    paginator = client.get_paginator('get_jobs')
    jobs: List[Dict[str, Any]] = []

    for page in paginator.paginate():
        jobs.extend(page['Jobs'])

    return jobs

@timeit
def list_all_interative_sessions(boto3_session: boto3.session.Session, region: str) -> List[Dict[str, Any]]:
    client = boto3_session.client('glue', region_name=region)
    sessions: List[Dict[str, Any]] = []

    response = client.list_sessions()
    sessions.extend(response.get('Sessions', []))

    return sessions

@timeit
def list_all_tables(boto3_session: boto3.session.Session, region: str, database_name:str) -> List[Dict[str, Any]]:
    client = boto3_session.client('glue', region_name=region)
    paginator =client.get_paginator('get_tables')
    tables: List[Dict[str, Any]] = []

    for page in paginator.paginate(DatabaseName=database_name):
        tables.extend(page['TableList'])

    return tables

@timeit
def list_all_connections(boto3_session: boto3.session.Session, region: str) -> List[Dict[str, Any]]:
    client = boto3_session.client('glue', region_name=region)

    paginator =client.get_paginator('get_connections')
    connections: List[Dict[str, Any]] = []

    for page in paginator.paginate():
        connections.extend(page['ConnectionList'])

    return connections


def extract_bucket_name_split(location_uri):
    if location_uri == "" or location_uri == None:
        return ""
    parts = location_uri.split('//')
    s3BucketName = parts[1].split("/")
    return s3BucketName[0]

def generate_glue_interactive_session_arn(region: str, account_id: str, interactive_session_id: str) -> str:

    """
    Return the glue interactive session arn.
    For example, return 'arn:aws:glue:us-east-1:123456789012:session/a1b2c3d4-5678-90ab-cdef-EXAMPLE11111'.
    :param region: AWS region code
    :param account_id : AWS account ID
    :param interactive_session_id : Session Id,
    :return: Glue ARN for the specified interactive session
    """

    return f"arn:aws:glue:{region}:{account_id}:session/{interactive_session_id}"

def generate_glue_connection_arn(region: str, account_id: str, connection_name: str) -> str:

    """
    Return the glue connection arn.
    For example, return ' arn:aws:glue:us-east-1:123456789012:connection/connection1'.
    :param region: AWS region code
    :param account_id : AWS account ID
    :param connection_name : Name of the Glue connection
    :return: Glue ARN for the specified connection
    """

    return f"arn:aws:glue:{region}:{account_id}:connection/{connection_name}"


def generate_glue_database_arn(region: str, account_id: str, database_name: str) -> str:

    """
    Return the glue database arn.
    For example, return ' arn:aws:glue:us-east-1:123456789012:database/db1'.
    :param region: AWS region code
    :param account_id : AWS account ID
    :param database_name : Name of the Glue database
    :return: Glue ARN for the specified database
    """

    return f"arn:aws:glue:{region}:{account_id}:database/{database_name}"

def generate_glue_table_arn(region: str, account_id: str, database_name: str, table_name: str)-> str:
    
    """
    Return the glue table arn.
    For example, return ' arn:aws:glue:us-east-1:123456789012:table/db1/tbl1'.
    :param region: AWS region code
    :param account_id : AWS account ID
    :param database_name : Name of the Glue database
    :param table_name : Name of the Table
    :return: Glue ARN for the specified table
    """

    return f"arn:aws:glue:{region}:{account_id}:table/{database_name}/{table_name}"

def generate_glue_crawler_arn(region: str, account_id: str, crawler_name: str) -> str:

    """
    Return the glue crawler arn.
    For example, return 'arn:aws:glue:us-east-1:123456789012:crawler/mycrawler'.
    :param region: AWS region code
    :param account_id : AWS account ID
    :param crawler_name : Name of the Glue crawler
    :return: Glue ARN for the specified crawler
    """

    return f"arn:aws:glue:{region}:{account_id}:crawler/{crawler_name}"

def generate_glue_job_arn(region: str, account_id: str, job_name: str) -> str:

    """
    Return the glue job arn.
    For example, return 'arn:aws:glue:us-east-1:123456789012:job/testjob'.
    :param region: AWS region code
    :param account_id : AWS account ID
    :param job_name : Name of the Glue Job 
    :return: Glue ARN for the specified job
    """

    return f"arn:aws:glue:{region}:{account_id}:job/{job_name}"

@timeit
def load_list_of_interactive_sessions(
        neo4j_session: neo4j.Session, sessions: List[Dict], region: str, current_aws_account_id: str, aws_update_tag: int,
) -> None:
    
    ingest_sessions = """
    MERGE (s:Glue:GlueInteractiveSession{arn: $SESSION_ARN})
    ON CREATE SET s.firstseen = timestamp()
    SET
    s.name = $SESSION_NAME,
    s.id = $SESSION_ID,
    s.createdOn = $SESSION_CREATED_ON,
    s.status = $SESSION_STATUS,
    s.errorMessage = $SESSION_ERROR_MESSAGE,
    s.description = $SESSION_DESCRIPTION,
    s.role = $SESSION_ROLE,
    s.pythonVersion = $SESSION_PYTHON_VERSION,
    s.connections = $SESSION_CONNECTIONS,
    s.progress = $SESSION_PROGRESS,
    s.maxCapacity = $SESSION_MAX_CAPACITY,
    s.securityConfiguration = $SESSION_SECURITY_CONFIGURATION,
    s.glueVersion = $SESSION_GLUE_VERSION,
    s.numberOfWorkers = $SESSION_NUMBER_OF_WORKERS,
    s.workerType = $SESSION_WORKER_TYPE,
    s.completedOn = $SESSION_COMPLETED_ON,
    s.executionTime = $SESSION_EXECUTION_TIME,
    s.dpuSeconds = $SESSION_DPU_SECONDS,
    s.idleTimeout = $SESSION_IDLE_TIMEOUT,
    s.region = $REGION,
    s.lastupdated = $aws_update_tag
    WITH s
    MATCH (aa:AWSAccount{id: $AWS_ACCOUNT_ID})
    MERGE (aa)-[r:RESOURCE]->(s)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = $aws_update_tag
    WITH s
    MATCH (role:AWSRole{arn: $SESSION_ROLE})
    MERGE(s)-[a:ATTACHED_ROLE]->(role)
    ON CREATE SET a.firstseen = timestamp()
    SET a.lastupdated = $aws_update_tag
    """
    for session in sessions:
        session_arn = generate_glue_interactive_session_arn(region, current_aws_account_id,session.get("Id", None))

        neo4j_session.run(
            ingest_sessions,
            SESSION_ARN=session_arn,
            SESSION_ID=session.get("Id", None),
            SESSION_CREATED_ON=session.get("CreatedOn", None),
            SESSION_STATUS=session.get("Status", None),
            SESSION_ERROR_MESSAGE=session.get("ErrorMessage", None),
            SESSION_DESCRIPTION=session.get("Description", None),
            SESSION_ROLE=session.get("Role", None),
            SESSION_NAME=session.get("Command",{}).get("Name",None),
            SESSION_PYTHON_VERSION=session.get("Command",{}).get("PythonVersion",None),
            SESSION_CONNECTIONS=session.get("Connections",{}).get("Connections",None),
            SESSION_PROGRESS=session.get("Progress", None),
            SESSION_MAX_CAPACITY=session.get("MaxCapacity", None),
            SESSION_SECURITY_CONFIGURATION=session.get("SecurityConfiguration", None),
            SESSION_GLUE_VERSION=session.get("GlueVersion", None),
            SESSION_NUMBER_OF_WORKERS=session.get("NumberOfWorkers", None),
            SESSION_WORKER_TYPE=session.get("WorkerType", None),
            SESSION_COMPLETED_ON=session.get("CompletedOn", None),
            SESSION_EXECUTION_TIME=session.get("ExecutionTime", None),
            SESSION_DPU_SECONDS=session.get("DPUSeconds", None),
            SESSION_IDLE_TIMEOUT=session.get("IdleTimeout", None),
            REGION=region,
            AWS_ACCOUNT_ID=current_aws_account_id,
            aws_update_tag=aws_update_tag,
        )

@timeit
def load_list_of_connections(
        neo4j_session: neo4j.Session, connections: List[Dict], region: str, current_aws_account_id: str, aws_update_tag: int,
) -> None:
    
    ingest_connections = """
    MERGE (c:Glue:GlueConnection{arn: $CONNECTION_ARN})
    ON CREATE SET c.firstseen = timestamp()
    SET
    c.name = $CONNECTION_NAME,
    c.description = $CONNECTION_DESCRIPTION,
    c.type = $CONNECTION_TYPE,
    c.subnetid = $CONNECTION_SUBNET_ID,
    c.securityGroupIdList = $CONNECTION_SECURITY_GROUP_ID_LIST,
    c.availabilityZone = $CONNECTION_AVAILABILITY_ZONE,
    c.creationTime = $CONNECTION_CREATION_TIME,
    c.lastUpdatedTime = $CONNECTION_LAST_UPDATED_TIME,
    c.lastUpdatedBy = $CONNECTION_LAST_UPDATED_BY,
    c.region = $REGION,
    c.lastupdated = $aws_update_tag
    WITH c
    MATCH (aa:AWSAccount{id: $AWS_ACCOUNT_ID})
    MERGE (aa)-[r:RESOURCE]->(c)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = $aws_update_tag
    """

    for connection in connections:
        connection_arn = generate_glue_connection_arn(region, current_aws_account_id,connection.get("Name", None))

        # connection_properties = connection.get("ConnectionProperties", None)
        # connection_properties_list = [{"key": key, "value": str(value)} for key, value in connection_properties.items()]

        neo4j_session.run(
            ingest_connections,
            CONNECTION_ARN = connection_arn,
            CONNECTION_NAME=connection.get("Name", None),
            CONNECTION_DESCRIPTION=connection.get("Description", None),
            CONNECTION_TYPE=connection.get("ConnectionType", None),
            # CONNECTION_PROPERTIES=connection_properties_list,
            CONNECTION_SUBNET_ID=connection.get("PhysicalConnectionRequirements",{}).get("SubnetId",None),
            CONNECTION_SECURITY_GROUP_ID_LIST=connection.get("PhysicalConnectionRequirements",{}).get("SecurityGroupIdList",None),
            CONNECTION_AVAILABILITY_ZONE=connection.get("PhysicalConnectionRequirements",{}).get("AvailabilityZone",None),
            CONNECTION_CREATION_TIME=connection.get("CreationTime", None),
            CONNECTION_LAST_UPDATED_TIME=connection.get("LastUpdatedTime", None),
            CONNECTION_LAST_UPDATED_BY=connection.get("LastUpdatedBy", None),
            REGION=region,
            AWS_ACCOUNT_ID=current_aws_account_id,
            aws_update_tag=aws_update_tag,
        )


@timeit
def load_list_of_jobs(
        neo4j_session: neo4j.Session, jobs: List[Dict], region: str, current_aws_account_id: str, aws_update_tag: int,
) -> None:
    
    ingest_jobs = """
    MERGE (j:Glue:GlueJob{arn: $JOB_ARN})
    ON CREATE SET j.firstseen = timestamp()
    SET
    j.name = $JOB_NAME,
    j.role = $JOB_ROLE,
    j.description = $JOB_DESCRIPTION,
    j.logUri = $JOB_LOG_URI,
    j.createdOn = $JOB_CREATED_ON,
    j.lastModifiedOn = $JOB_LAST_MODIFIED_ON,
    j.executionPropertyMaxConcurrentRuns = $JOB_EXECUTION_PROPERTY_MAX_CONCURRENT_RUNS,
    j.commandName = $JOB_COMMAND_NAME,
    j.commandScriptLocation = $JOB_COMMAND_SCRIPT_LOCATION,
    j.commandPythonVersion = $JOB_COMMAND_PYTHON_VERSION,
    j.commandRuntime = $JOB_COMMAND_RUNTIME,
    j.maxRetries = $JOB_MAX_RETRIES,
    j.allocatedCapacity = $JOB_ALLOCATED_CAPACITY,
    j.timeout = $JOB_TIMEOUT,
    j.maxCapacity = $JOB_MAX_CAPACITY,
    j.workerType = $JOB_WORKER_TYPE,
    j.numberOfWorkers = $JOB_NUMBER_OF_WORKERS,
    j.securityConfiguration = $JOB_SECURITY_CONFIGURATION,
    j.notificationProperty = $JOB_NOTIFICATION_PROPERTY,
    j.glueVersion = $JOB_GLUE_VERSION,
    j.executionClass = $JOB_EXECUTION_CLASS,
    j.jobSourceControlProvider = $JOB_SOURCE_CONTROL_DETAILS_PROVIDER,
    j.jobSourceControlRepository = $JOB_SOURCE_CONTROL_DETAILS_REPOSITORY,
    j.jobSourceControlOwner = $JOB_SOURCE_CONTROL_DETAILS_OWNER,
    j.jobSourceControlBranch = $JOB_SOURCE_CONTROL_DETAILS_BRANCH,
    j.jobSourceControlFolder = $JOB_SOURCE_CONTROL_DETAILS_FOLDER,
    j.jobSourceControlLastCommitId = $JOB_SOURCE_CONTROL_DETAILS_LAST_COMMIT_ID,
    j.jobSourceControlAuthStrategy = $JOB_SOURCE_CONTROL_DETAILS_AUTH_STRATEGY,
    j.jobSourceControlAuthToken = $JOB_SOURCE_CONTROL_DETAILS_AUTH_TOKEN,
    j.region = $REGION,
    j.lastupdated = $aws_update_tag
    WITH j
    MATCH (aa:AWSAccount{id: $AWS_ACCOUNT_ID})
    MERGE (aa)-[r:RESOURCE]->(j)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = $aws_update_tag
    WITH j
    MATCH (role:AWSRole{arn: $JOB_ROLE})
    MERGE(j)-[s:ATTACHED_ROLE]->(role)
    ON CREATE SET s.firstseen = timestamp()
    SET s.lastupdated = $aws_update_tag
    """

    job_to_s3_query = """
        MATCH (s3:S3Bucket{id: $S3_BUCKET_ID})
        MATCH (job:GlueJob{arn: $JOB_ARN})
        MERGE (job)-[h:HAS_ACCESS]->(s3)
        ON CREATE SET h.firstseen = timestamp()
        SET h.lastupdated = $aws_update_tag
    """

    for job in jobs:
        location_uri = job["Command"]["ScriptLocation"]
        bucket_name = extract_bucket_name_split(location_uri)
        job_arn = generate_glue_job_arn(region, current_aws_account_id,job.get("Name", None))

        neo4j_session.run(
            ingest_jobs,
            JOB_ARN = job_arn,
            JOB_NAME=job.get("Name", None),
            JOB_DESCRIPTION=job.get("Description", None),
            JOB_LOG_URI=job.get("LogUri", None),
            JOB_ROLE=job.get("Role", None),
            JOB_CREATED_ON=job.get("CreatedOn", None),
            JOB_LAST_MODIFIED_ON=job.get("LastModifiedOn", None),
            JOB_EXECUTION_PROPERTY_MAX_CONCURRENT_RUNS=job.get("ExecutionProperty", {}).get("MaxConcurrentRuns", None),
            JOB_COMMAND_NAME=job.get("Command",{}).get("Name", None),
            JOB_COMMAND_SCRIPT_LOCATION=job.get("Command",{}).get("ScriptLocation", None),
            JOB_COMMAND_PYTHON_VERSION=job.get("Command",{}).get("PythonVersion", None),
            JOB_COMMAND_RUNTIME=job.get("Command",{}).get("Runtime", None),
            JOB_MAX_RETRIES=job.get("MaxRetries", None),
            JOB_ALLOCATED_CAPACITY=job.get("AllocatedCapacity", None),
            JOB_TIMEOUT=job.get("Timeout", None),
            JOB_MAX_CAPACITY=job.get("MaxCapacity", None),
            JOB_WORKER_TYPE=job.get("WorkerType", None),
            JOB_NUMBER_OF_WORKERS=job.get("NumberOfWorkers", None),
            JOB_SECURITY_CONFIGURATION=job.get("SecurityConfiguration", None),
            JOB_NOTIFICATION_PROPERTY=job.get("NotificationProperty",{}).get("NotifyDelayAfter",None),
            JOB_GLUE_VERSION=job.get("GlueVersion", None),
            JOB_EXECUTION_CLASS=job.get("ExecutionClass", None),
            JOB_SOURCE_CONTROL_DETAILS_PROVIDER=job.get("SourceControlDetails",{}).get("Provider",None),
            JOB_SOURCE_CONTROL_DETAILS_REPOSITORY=job.get("SourceControlDetails",{}).get("Repository",None),
            JOB_SOURCE_CONTROL_DETAILS_OWNER=job.get("SourceControlDetails",{}).get("Owner",None),
            JOB_SOURCE_CONTROL_DETAILS_BRANCH=job.get("SourceControlDetails",{}).get("Branch",None),
            JOB_SOURCE_CONTROL_DETAILS_FOLDER=job.get("SourceControlDetails",{}).get("Folder",None),
            JOB_SOURCE_CONTROL_DETAILS_LAST_COMMIT_ID=job.get("SourceControlDetails",{}).get("LastCommitId",None),
            JOB_SOURCE_CONTROL_DETAILS_AUTH_STRATEGY=job.get("SourceControlDetails",{}).get("AuthStrategy",None),
            JOB_SOURCE_CONTROL_DETAILS_AUTH_TOKEN=job.get("SourceControlDetails",{}).get("AuthToken",None),
            S3_BUCKET_NAME=bucket_name,
            REGION=region,
            AWS_ACCOUNT_ID=current_aws_account_id,
            aws_update_tag=aws_update_tag,
        )

        if bucket_name is not None:
            neo4j_session.run(
                job_to_s3_query,
                S3_BUCKET_ID=bucket_name,
                JOB_ARN = job_arn,
                aws_update_tag=aws_update_tag,
            )

@timeit
def load_list_of_tables(
        neo4j_session: neo4j.Session, tables: List[Dict], region: str, current_aws_account_id: str, aws_update_tag: int,
) -> None:
    
    ingest_table = """
    MERGE (t:Glue:GlueTable{arn: $TABLE_ARN})
    ON CREATE SET t.firstseen = timestamp()
    SET t.databaseName = $TABLE_DATABASE_NAME,
    t.tableName = $TABLE_NAME,
    t.description = $TABLE_DESCRIPTION,
    t.owner = $TABLE_OWNER,
    t.createTime = $TABLE_CREATE_TIME,
    t.updateTime = $TABLE_UPDATE_TIME,
    t.lastAccessTime = $TABLE_LAST_ACCESS_TIME,
    t.lastAnalyzedTime = $TABLE_LAST_ANALYZED_TIME,
    t.retention = $TABLE_RETENTION,
    t.viewOriginalText = $TABLE_VIEW_ORIGINAL_TEXT,
    t.viewExpandedText = $TABLE_VIEW_EXPANDED_TEXT,
    t.tableType = $TABLE_TYPE,
    t.createdBy = $TABLE_CREATED_BY,
    t.isRegisteredWithLakeFormation = $TABLE_IS_REGISTERED_WITH_LAKE_FORMATION,
    t.targetTableCatalogId = $TARGET_TABLE_CATALOG_ID,
    t.targetTableDatabaseName = $TARGET_TABLE_DATABASE_NAME,
    t.targetTableName = $TARGET_TABLE_NAME,
    t.targetTableRegion = $TARGET_TABLE_REGION,
    t.catalogId = $TABLE_CATALOG_ID,
    t.versionId = $TABLE_VERSION_ID,
    t.federatedTableIdentifier = $FEDERATED_TABLE_IDENTIFIER,
    t.federatedTableDatabaseIdentifier = $FEDERATED_TABLE_DATABASE_IDENTIFIER,
    t.federatedTableConnectionName = $FEDERATED_TABLE_CONNECTION_NAME,
    t.region = $REGION,
    t.lastupdated = $aws_update_tag
    WITH t
    MATCH (db:GlueDatabase{databaseName: $TABLE_DATABASE_NAME})
    MERGE (db)-[:CONTAINS]->(t)
    ON CREATE SET db.firstseen = timestamp()
    SET db.lastupdated = $aws_update_tag
    """

    for table in tables:

        table_arn = generate_glue_table_arn(region, current_aws_account_id, table.get("DatabaseName", None), table.get("Name", None))

        neo4j_session.run(
            ingest_table,
            TABLE_ARN = table_arn,
            TABLE_NAME=table.get("Name", None),
            TABLE_DATABASE_NAME=table.get("DatabaseName", None),
            TABLE_DESCRIPTION=table.get("Description", None),
            TABLE_OWNER=table.get("Owner", None),
            TABLE_CREATE_TIME=table.get("CreateTime", None),
            TABLE_UPDATE_TIME=table.get("UpdateTime", None),
            TABLE_LAST_ACCESS_TIME=table.get("LastAccessTime", None),
            TABLE_LAST_ANALYZED_TIME=table.get("LastAnalyzedTime", None),
            TABLE_RETENTION=table.get("Retention", None),
            TABLE_VIEW_ORIGINAL_TEXT=table.get("ViewOriginalText", None),
            TABLE_VIEW_EXPANDED_TEXT=table.get("ViewExpandedText", None),
            TABLE_TYPE=table.get("TableType", None),
            TABLE_CREATED_BY=table.get("CreatedBy", None),
            TABLE_IS_REGISTERED_WITH_LAKE_FORMATION=table.get("IsRegisteredWithLakeFormation", None),
            TARGET_TABLE_CATALOG_ID=table.get("TargetTable", {}).get("CatalogId", None),
            TARGET_TABLE_DATABASE_NAME=table.get("TargetTable", {}).get("DatabaseName", None),
            TARGET_TABLE_NAME=table.get("TargetTable", {}).get("Name", None),
            TARGET_TABLE_REGION=table.get("TargetTable", {}).get("Region", None),
            TABLE_CATALOG_ID=table.get("CatalogId", None),
            TABLE_VERSION_ID=table.get("VersionId", None),
            FEDERATED_TABLE_IDENTIFIER=table.get("FederatedTable", {}).get("Identifier", None),
            FEDERATED_TABLE_DATABASE_IDENTIFIER=table.get("FederatedTable", {}).get("DatabaseIdentifier", None),
            FEDERATED_TABLE_CONNECTION_NAME=table.get("FederatedTable", {}).get("ConnectionName", None),
            REGION=region,
            AWS_ACCOUNT_ID=current_aws_account_id,
            aws_update_tag=aws_update_tag,
        )
    
@timeit
def load_list_of_databases(
    neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, databases: List[Dict], region: str, current_aws_account_id: str, aws_update_tag: int,
) -> None:

    ingest_database = """
    MERGE (db:Glue:GlueDatabase{arn: $DATABASE_ARN})
    ON CREATE SET db.databaseName = $DATABASE_NAME, db.firstseen = timestamp()
    SET db.createTime = $DATABASE_CREATE_TIME,
    db.catalog_id = $DATABASE_CATALOG_ID,
    db.createTableDefaultPermissions = $DATABASE_CREATE_TABLE_DEFAULT_PERMISSIONS.Permissions,
    db.createTableDefaultPrincipal = $DATABASE_CREATE_TABLE_DEFAULT_PERMISSIONS.Principal.DataLakePrincipalIdentifier,
    db.description= $DATABASE_DESCRIPTION,
    db.locationUri = $DATABASE_LOCATION_URI,
    db.targetDatabaseCatalogId = $TARGET_DATABASE_CATALOG_ID,
    db.targetDatabaseName = $TARGET_DATABASE_NAME,
    db.targetDatabaseRegion = $TARGET_DATABASE_REGION,
    db.federatedDatabaseIdentifier = $FEDERATED_DATABASE_IDENTIFIER,
    db.federatedDatabaseConnectionName = $FEDERATED_DATABASE_CONNECTION_NAME,
    db.region = $REGION,
    db.lastupdated = $aws_update_tag
    WITH db
    MATCH (aa:AWSAccount{id: $AWS_ACCOUNT_ID})
    MERGE (aa)-[r:RESOURCE]->(db)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = $aws_update_tag
    """

    database_to_s3_query = """
    MATCH (s3:S3Bucket{id: $S3_BUCKET_NAME})
    MATCH(db:GlueDatabase{arn: $DATABASE_ARN})
    MERGE (db)-[r:HAS_ACCESS]->(s3)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = $aws_update_tag
    """


    for database in databases:
        location_uri = database["LocationUri"]
        bucket_name = extract_bucket_name_split(location_uri)
        database_arn = generate_glue_database_arn(region, current_aws_account_id,database.get("Name", None))

        neo4j_session.run(
            ingest_database,
            DATABASE_ARN=database_arn,
            DATABASE_NAME=database.get("Name", None),
            DATABASE_DESCRIPTION=database.get("Description", None),
            DATABASE_LOCATION_URI=database.get("LocationUri", None),
            DATABASE_CREATE_TIME=database.get("CreateTime", None),
            DATABASE_CREATE_TABLE_DEFAULT_PERMISSIONS = {
                "Permissions": database.get("CreateTableDefaultPermissions", [{}])[0].get("Permissions", None),
                "Principal": {
                    "DataLakePrincipalIdentifier": database.get("CreateTableDefaultPermissions", [{}])[0]
                        .get("Principal", {})
                        .get("DataLakePrincipalIdentifier", None)
                }
            },
            TARGET_DATABASE_CATALOG_ID=database.get("TargetDatabase", {}).get("CatalogId", None),
            TARGET_DATABASE_NAME=database.get("TargetDatabase", {}).get("DatabaseName", None),
            TARGET_DATABASE_REGION=database.get("TargetDatabase", {}).get("Region", None),
            DATABASE_CATALOG_ID=database.get("CatalogId", None),
            FEDERATED_DATABASE_IDENTIFIER=database.get("FederatedDatabase", {}).get("Identifier", None),
            FEDERATED_DATABASE_CONNECTION_NAME=database.get("FederatedDatabase", {}).get("ConnectionName", None),
            S3_BUCKET_NAME=bucket_name,
            REGION=region,
            AWS_ACCOUNT_ID=current_aws_account_id,
            aws_update_tag=aws_update_tag,
        )

        if bucket_name is not None:
            neo4j_session.run(
                database_to_s3_query,
                S3_BUCKET_NAME=bucket_name,
                DATABASE_ARN=database_arn,
                aws_update_tag=aws_update_tag
            )

    tables = []
    for database in databases:
        tables.extend(list_all_tables(boto3_session, region, database["Name"]))
        pprint(tables)
        load_list_of_tables(neo4j_session,tables,region,current_aws_account_id,aws_update_tag)


@timeit
def load_list_of_crawlers(
    neo4j_session: neo4j.Session, crawlers: List[Dict], region: str, current_aws_account_id: str, aws_update_tag: int,
) -> None:
    ingest_crawler = """
    MERGE (c:Glue:GlueCrawler{arn: $CRAWLER_ARN})
    ON CREATE SET c.firstseen = timestamp()
    SET c.role = $CRAWLER_ROLE,
    c.name = $CRAWLER_NAME,
    c.databaseName = $CRAWLER_DATABASE_NAME,
    c.description = $CRAWLER_DESCRIPTION,
    c.classifiers = $CRAWLER_CLASSIFIERS,
    c.recrawlPolicy = $CRAWLER_RECRAWL_POLICY.RecrawlBehavior,
    c.schemaChangePolicyUpdateBehaviour = $CRAWLER_SCHEMA_CHANGE_POLICY_UPDATE_BEHAVIOUR,
    c.schemaChangePolicyDeleteBehaviour = $CRAWLER_SCHEMA_CHANGE_POLICY_DELETE_BEHAVIOUR,
    c.linearConfiguration = $CRAWLER_LINEAR_CONFIGURATION_LINEAR_SETTINGS,
    c.state = $CRAWLER_STATE,
    c.tablePrefix = $CRAWLER_TABLE_PREFIX,
    c.scheduleExpression = $CRAWLER_SCHEDULE_EXPRESSION,
    c.scheduleState = $CRAWLER_SCHEDULE_STATE,
    c.crawlElapsedTime = $CRAWLER_CRAWL_ELAPSED_TIME,
    c.creationTime = $CRAWLER_CREATION_TIME,
    c.lastUpdated = $CRAWLER_LAST_UPDATED,
    c.lastCrawlStartTime = $CRAWLER_LAST_CRAWL_START_TIME,
    c.lastCrawlStatus = $CRAWLER_LAST_CRAWL_STATUS,
    c.lastCrawlLogGroup = $CRAWLER_LAST_CRAWL_LOG_GROUP,
    c.lastCrawlLogStream = $CRAWLER_LAST_CRAWL_LOG_STREAM,
    c.lastCrawlMessagePrefix = $CRAWLER_LAST_CRAWL_MESSAGE_PREFIX,
    c.lastCrawlErrorMessage = $CRAWLER_LAST_CRAWL_ERROR_MESSAGE,
    c.version = $CRAWLER_VERSION,
    c.configuration = $CRAWLER_CONFIGURATION,
    c.securityConfiguration = $CRAWLER_SECURITY_CONFIGURATION,
    c.lakeFormationConfigurationUseCredentials = $LAKE_FORMATION_CONFIGURATION_USE_CREDENTIALS,
    c.lakeFormationConfigurationAccountId = $LAKE_FORMATION_CONFIGURATION_ACCOUNT_ID,
    c.region = $REGION,
    c.lastupdated = $aws_update_tag
    WITH c
    MATCH (aa:AWSAccount{id: $AWS_ACCOUNT_ID})
    MERGE (aa)-[r:RESOURCE]->(c)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = $aws_update_tag
    WITH c
    MATCH (role:AWSRole{name: $CRAWLER_ROLE})
    MERGE(c)-[s:ATTACHED_ROLE]->(role)
    ON CREATE SET s.firstseen = timestamp()
    SET s.lastupdated = $aws_update_tag
    """

    crawler_to_database = """
    MATCH (d:GlueDatabase{databaseName: $CRAWLER_DATABASE_NAME})
    MATCH (c:GlueCrawler{arn: $CRAWLER_ARN})
    MERGE (c)-[t:RESOURCE]->(d)
    ON CREATE SET t.firstseen = timestamp()
    SET t.lastupdated = $aws_update_tag
    """

    for crawler in crawlers:

        crawler_arn = generate_glue_crawler_arn(region, current_aws_account_id,crawler.get("Name", None))

        neo4j_session.run(
            ingest_crawler,
            CRAWLER_ARN = crawler_arn,
            CRAWLER_NAME=crawler.get("Name", None),
            CRAWLER_ROLE=crawler.get("Role", None),
            CRAWLER_DATABASE_NAME=crawler.get("DatabaseName", None),
            CRAWLER_DESCRIPTION=crawler.get("Description", None),
            CRAWLER_CLASSIFIERS=crawler.get("Classifiers", None),
            CRAWLER_RECRAWL_POLICY=crawler.get("RecrawlPolicy", None), 
            CRAWLER_SCHEMA_CHANGE_POLICY_UPDATE_BEHAVIOUR=crawler.get("SchemaChangePolicy", {}).get("UpdateBehavior", None),
            CRAWLER_SCHEMA_CHANGE_POLICY_DELETE_BEHAVIOUR=crawler.get("SchemaChangePolicy", {}).get("DeleteBehavior", None),
            CRAWLER_LINEAR_CONFIGURATION_LINEAR_SETTINGS=crawler.get("LineageConfiguration", {}).get("CrawlerLineageSettings", None),
            CRAWLER_STATE=crawler.get("State", None),
            CRAWLER_TABLE_PREFIX=crawler.get("TablePrefix", None),
            CRAWLER_SCHEDULE_EXPRESSION=crawler.get("Schedule", {}).get("ScheduleExpression", None),
            CRAWLER_SCHEDULE_STATE=crawler.get("Schedule", {}).get("State", None),
            CRAWLER_CRAWL_ELAPSED_TIME=crawler.get("CrawlElapsedTime", None),
            CRAWLER_CREATION_TIME=crawler.get("CreationTime", None),
            CRAWLER_LAST_UPDATED=crawler.get("LastUpdated", None),
            CRAWLER_LAST_CRAWL_START_TIME=crawler.get("LastCrawl", {}).get("StartTime", None),
            CRAWLER_LAST_CRAWL_STATUS=crawler.get("LastCrawl", {}).get("Status", None),
            CRAWLER_LAST_CRAWL_LOG_GROUP=crawler.get("LastCrawl", {}).get("LogGroup", None),
            CRAWLER_LAST_CRAWL_LOG_STREAM=crawler.get("LastCrawl", {}).get("LogStream", None),
            CRAWLER_LAST_CRAWL_MESSAGE_PREFIX=crawler.get("LastCrawl", {}).get("MessagePrefix", None),
            CRAWLER_LAST_CRAWL_ERROR_MESSAGE=crawler.get("LastCrawl", {}).get("ErrorMessage", None),
            CRAWLER_VERSION=crawler.get("Version", None),
            CRAWLER_CONFIGURATION=crawler.get("Configuration", None),
            CRAWLER_SECURITY_CONFIGURATION=crawler.get("CrawlerSecurityConfiguration", None),
            LAKE_FORMATION_CONFIGURATION_USE_CREDENTIALS=crawler.get("LakeFormationConfiguration", {}).get("UseLakeFormationCredentials", None),
            LAKE_FORMATION_CONFIGURATION_ACCOUNT_ID=crawler.get("LakeFormationConfiguration", {}).get("AccountId", None),
            REGION=region,
            AWS_ACCOUNT_ID=current_aws_account_id,
            aws_update_tag=aws_update_tag,
        )

        neo4j_session.run(
            crawler_to_database,
            CRAWLER_DATABASE_NAME=crawler.get("DatabaseName", None),
            CRAWLER_ARN = crawler_arn,
            aws_update_tag=aws_update_tag,
        )

@timeit
def sync_crawlers_for_region(
    neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, region: str, current_aws_account_id: str,
    aws_update_tag: int, common_job_parameters: Dict,
) -> None:
    logger.info("Syncing Glue for account '%s'.", current_aws_account_id)

    # logger.info("TRYING TO RETRIVE LIST OF CRAWLERS")

    crawlers = list_all_crawlers(boto3_session, region)
    pprint(crawlers)
    load_list_of_crawlers(neo4j_session, crawlers, region, current_aws_account_id, aws_update_tag)

    logger.info("List of Crawlers retrieved")

@timeit
def sync_connections_for_region(
    neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, region: str, current_aws_account_id: str,
    aws_update_tag: int, common_job_parameters: Dict,
) -> None:
    logger.info("Syncing Glue for account '%s'.", current_aws_account_id)

    # logger.info("TRYING TO RETRIVE LIST OF CONNECTIONS")

    connections = list_all_connections(boto3_session, region)
    pprint(connections)
    load_list_of_connections(neo4j_session,connections, region, current_aws_account_id, aws_update_tag)

    logger.info("List of Connections retrieved")

@timeit
def sync_interactive_sessions_for_region(
    neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, region: str, current_aws_account_id: str,
    aws_update_tag: int, common_job_parameters: Dict,
) -> None:
    logger.info("Syncing Glue for account '%s'.", current_aws_account_id)

    # logger.info("TRYING TO RETRIVE LIST OF INTERACTIVE SESSIONS")

    sessions = list_all_interative_sessions(boto3_session, region)
    pprint(sessions)
    load_list_of_interactive_sessions(neo4j_session, sessions, region, current_aws_account_id, aws_update_tag)

    logger.info("List of Interactive Sessions retrieved")

@timeit
def sync_databases_for_region(
    neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, region: str, current_aws_account_id: str,
    aws_update_tag: int, common_job_parameters: Dict,
) -> None:
    logger.info("Syncing Glue for account '%s'.", current_aws_account_id)

    # logger.info("TRYING TO RETRIVE LIST OF DATABASES")

    databases = list_all_databases(boto3_session, region)
    pprint(databases)
    load_list_of_databases(neo4j_session,boto3_session, databases, region, current_aws_account_id, aws_update_tag)

    logger.info("List of Databases retrieved")


@timeit
def sync_jobs_for_region(
    neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, region: str, current_aws_account_id: str,
    aws_update_tag: int, common_job_parameters: Dict,
) -> None:
    logger.info("Syncing Glue for account '%s'.", current_aws_account_id)

    # logger.info("TRYING TO RETRIVE LIST OF JOBS")

    jobs = list_all_jobs(boto3_session, region)
    pprint(jobs)
    load_list_of_jobs(neo4j_session, jobs, region, current_aws_account_id, aws_update_tag)

    logger.info("List of Jobs retrieved")


@timeit
def cleanup_glue(neo4j_session: neo4j.Session, common_job_parameters: Dict) -> None:
    run_cleanup_job('aws_import_glue_cleanup.json', neo4j_session, common_job_parameters)


@timeit
def sync(
    neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str], current_aws_account_id: str,
    update_tag: int, common_job_parameters: Dict,
) -> None:
    logger.info("Syncing Glue for account '%s'.", current_aws_account_id)

    for region in regions:
        sync_databases_for_region(neo4j_session, boto3_session, region, current_aws_account_id, update_tag, common_job_parameters)
        sync_jobs_for_region(neo4j_session, boto3_session, region, current_aws_account_id, update_tag, common_job_parameters)
        sync_crawlers_for_region(neo4j_session, boto3_session, region, current_aws_account_id, update_tag, common_job_parameters)
        sync_connections_for_region(neo4j_session, boto3_session, region, current_aws_account_id, update_tag, common_job_parameters)
        sync_interactive_sessions_for_region(neo4j_session, boto3_session, region, current_aws_account_id, update_tag, common_job_parameters)

    cleanup_glue(neo4j_session, common_job_parameters)



