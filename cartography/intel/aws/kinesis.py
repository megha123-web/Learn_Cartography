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
def get_resource_policy(kinesis_client: boto3.client, data_stream_arn: str) -> str:
    try:
        response = kinesis_client.get_resource_policy(ResourceARN = data_stream_arn)
        return response.get('Policy')
    except kinesis_client.exceptions.ResourceNotFoundException:
        return ('No resource policy to display')
    
@timeit
def list_all_data_streams(boto3_session: boto3.session.Session, region: str) -> List[Dict[str, Any]]:
    client = boto3_session.client('kinesis', region_name=region)
    paginator = client.get_paginator('list_streams')
    data_streams: List[Dict[str, Any]] = []

    for page in paginator.paginate():
        data_streams.extend(page['StreamSummaries'])

    for data_stream in data_streams:
        resource_policy = get_resource_policy(client, data_stream['StreamARN'])
        data_stream['ResourcePolicy'] = resource_policy

    return data_streams

@timeit
def list_all_video_streams(boto3_session: boto3.session.Session, region: str) -> List[Dict[str, Any]]:
    client = boto3_session.client('kinesisvideo', region_name=region)
    paginator = client.get_paginator('list_streams')
    video_streams: List[Dict[str, Any]] = []

    for page in paginator.paginate():
        video_streams.extend(page['StreamInfoList'])

    return video_streams

@timeit
def load_list_of_data_streams(
    neo4j_session: neo4j.Session, data_streams: List[Dict], region: str, current_aws_account_id: str, aws_update_tag: int,
) -> None:
    ingest_user = """

    MERGE (unode:KinesisDataStream{arn: $STREAM_ARN})
    ON CREATE SET unode.streamName = $STREAM_NAME, unode.firstseen = timestamp()
    SET unode.streamStatus = $STREAM_STATUS,
    unode.streamMode = $STREAM_MODE,
    unode.streamCreationTimestamp= $STREAM_CREATION_TIMESTAMP,
    unode.resourcePolicy = $RESOURCE_POLICY,
    unode.region = $REGION,
    unode.lastupdated = $aws_update_tag
    WITH unode
    MATCH (aa:AWSAccount{id: $AWS_ACCOUNT_ID})
    MERGE (aa)-[r:RESOURCE]->(unode)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = $aws_update_tag
    """

    for data_stream in data_streams:
        # pprint(data_stream)
        neo4j_session.run(
            ingest_user,
            STREAM_NAME=data_stream["StreamName"],
            STREAM_ARN=data_stream["StreamARN"],
            STREAM_STATUS=data_stream["StreamStatus"],
            STREAM_MODE=data_stream["StreamModeDetails"]["StreamMode"],
            STREAM_CREATION_TIMESTAMP=data_stream.get("StreamCreationTimestamp", None),
            RESOURCE_POLICY=data_stream['ResourcePolicy'],
            REGION=region,
            AWS_ACCOUNT_ID=current_aws_account_id,
            aws_update_tag=aws_update_tag,
        )

@timeit
def load_list_of_video_streams(
    neo4j_session: neo4j.Session, video_streams: List[Dict], region: str, current_aws_account_id: str, aws_update_tag: int,
) -> None:
    ingest_user = """

    MERGE (unode:KinesisVideoStream{arn: $STREAM_ARN})
    ON CREATE SET unode.streamName = $STREAM_NAME, unode.firstseen = timestamp()
    SET unode.deviceName = $DEVICE_NAME,
    unode.mediaType = $MEDIA_TYPE,
    unode.kmsKeyId = $KMS_KEY_ID,
    unode.version = $VERSION,
    unode.status = $STATUS,
    unode.creationTime = $CREATION_TIME,
    unode.dataRetentionInHours = $DATA_RETENTION_IN_HOURS,
    unode.region = $REGION,
    unode.lastupdated = $aws_update_tag
    WITH unode
    MATCH (aa:AWSAccount{id: $AWS_ACCOUNT_ID})
    MERGE (aa)-[r:RESOURCE]->(unode)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = $aws_update_tag
    """

    for video_stream in video_streams:
        neo4j_session.run(
            ingest_user,
            DEVICE_NAME=video_stream.get("DeviceName", None),
            STREAM_NAME=video_stream["StreamName"],
            STREAM_ARN=video_stream["StreamARN"],
            MEDIA_TYPE=video_stream.get("MediaName", None),
            KMS_KEY_ID=video_stream["KmsKeyId"],
            VERSION=video_stream["Version"],
            STATUS=video_stream["Status"],
            CREATION_TIME=video_stream.get("CreationTime", None),
            DATA_RETENTION_IN_HOURS=video_stream.get("DataRetentionInHours", None),
            REGION=region,
            AWS_ACCOUNT_ID=current_aws_account_id,
            aws_update_tag=aws_update_tag,
        )


@timeit
def cleanup_kinesis(neo4j_session: neo4j.Session, common_job_parameters: Dict) -> None:
    run_cleanup_job('aws_import_kinesis_cleanup.json', neo4j_session, common_job_parameters)


@timeit
def sync_data_streams_for_region(
    neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, region: str, current_aws_account_id: str,
    aws_update_tag: int, common_job_parameters: Dict,
) -> None:
    logger.info("Syncing Kinesis for account '%s'.", current_aws_account_id)

    # logger.info("TRYING TO RETRIVE LIST OF DATA STREAMS")

    data_streams = list_all_data_streams(boto3_session, region)
    pprint(data_streams)
    load_list_of_data_streams(neo4j_session, data_streams, region, current_aws_account_id, aws_update_tag)

    logger.info("List of Data Streams retrieved")


@timeit
def sync_video_streams_for_region(
    neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, region: str, current_aws_account_id: str,
    aws_update_tag: int, common_job_parameters: Dict,
) -> None:
    logger.info("Syncing Kinesis for account '%s'.", current_aws_account_id)

    # logger.info("TRYING TO RETRIVE LIST OF VIDEO STREAMS")

    video_streams = list_all_video_streams(boto3_session, region)
    pprint(video_streams)
    load_list_of_video_streams(neo4j_session, video_streams, region, current_aws_account_id, aws_update_tag)

    logger.info("List of Video Streams retrieved")


@timeit
def sync(
    neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str], current_aws_account_id: str,
    update_tag: int, common_job_parameters: Dict,
) -> None:
    logger.info("Syncing Kinesis for account '%s'.", current_aws_account_id)

    service_region = ["northeast-1", "northeast-3", "ap-northeast-3", "us-west-1"]

    for region in regions:
        if region in service_region:
            continue

        sync_data_streams_for_region(neo4j_session, boto3_session, region, current_aws_account_id, update_tag, common_job_parameters)
        sync_video_streams_for_region(neo4j_session, boto3_session, region, current_aws_account_id, update_tag, common_job_parameters)

    cleanup_kinesis(neo4j_session, common_job_parameters)


