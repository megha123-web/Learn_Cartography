import logging
import json
from typing import Any
from typing import Dict
from typing import List
from pprint import pprint

import botocore
import boto3
import neo4j

from cartography.util import run_cleanup_job
from cartography.stats import get_stats_client
from cartography.util import timeit


logger = logging.getLogger(__name__)
stat_handler = get_stats_client(__name__)

@timeit
def get_vault_access_policy(glacier_client: Any, vault_name: str) -> str:
    try:
        response = glacier_client.get_vault_access_policy(vaultName=vault_name)
        return response.get('policy')
    except glacier_client.exceptions.ResourceNotFoundException:
        return ('No vault access policy to display')
    
@timeit   
def list_all_vaults(boto3_session: boto3.session.Session, region: str) -> List[Dict[str, Any]]:
    client = boto3_session.client('glacier', region_name=region)

    paginator = client.get_paginator('list_vaults')
    vaults: List[Dict[str, Any]] = []

    for page in paginator.paginate():
        vaults.extend(page['VaultList'])

    for vault in vaults:
        access_policy = get_vault_access_policy(client, vault['VaultName'])
        vault['AccessPolicy'] = json.dumps(access_policy)

    return vaults

@timeit 
def load_list_of_vaults(
    neo4j_session: neo4j.Session, vaults: List[Dict], region: str, current_aws_account_id: str, aws_update_tag: int,
) -> None:
    ingest_user = """
    
    MERGE (unode:S3GlacierVault{arn: $VAULT_ARN})
    ON CREATE SET unode.vaultname = $VAULT_NAME, unode.firstseen = timestamp()
    SET unode.creationDate = $CREATION_DATE, 
    unode.lastInventoryDate = $LAST_INVENTORY_DATE, 
    unode.numberOfArchives= $NUMBER_OF_ARCHIVES, 
    unode.sizeInBytes = $SIZE_IN_BYTES,
    unode.accessPolicy = $ACCESS_POLICY,
    unode.region = $REGION,
    unode.lastupdated = $aws_update_tag
    WITH unode
    MATCH (aa:AWSAccount{id: $AWS_ACCOUNT_ID})
    MERGE (aa)-[r:RESOURCE]->(unode)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = $aws_update_tag
    """

    print("LOADINGGGGGGGGGGGGGGG")

    for vault in vaults:
        pprint(vault)
        neo4j_session.run(
            ingest_user,
            VAULT_ARN=vault["VaultARN"],
            VAULT_NAME=vault["VaultName"],
            CREATION_DATE=vault["CreationDate"],
            LAST_INVENTORY_DATE=vault.get("LastInventoryDate",None),
            NUMBER_OF_ARCHIVES=vault["NumberOfArchives"],
            SIZE_IN_BYTES=vault["SizeInBytes"],
            ACCESS_POLICY=vault['AccessPolicy'],
            REGION=region,
            AWS_ACCOUNT_ID=current_aws_account_id,
            aws_update_tag=aws_update_tag,
        )

@timeit 
def cleanup_s3_glacier(neo4j_session: neo4j.Session, common_job_parameters: Dict) -> None:
    run_cleanup_job('aws_import_s3_glacier_cleanup.json', neo4j_session, common_job_parameters)   


@timeit 
def sync_vaults(
    neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, region: str, current_aws_account_id: str,
    aws_update_tag: int, common_job_parameters: Dict,
    ) -> None:
    logger.info("Syncing S3 Glacier for account '%s'.", current_aws_account_id)
    
    logger.info("TRYING TO RETRIVE LIST OF VAULTS")

    vaults = list_all_vaults(boto3_session,region)
    print(vaults)
    load_list_of_vaults(neo4j_session, vaults, region, current_aws_account_id, aws_update_tag)

    logger.info("List of Vaults retrieved")

@timeit 
def sync(
    neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str], current_aws_account_id: str,
    update_tag: int, common_job_parameters: Dict,
) -> None:
    logger.info("Syncing S3 Glacier for account '%s'.", current_aws_account_id)

    for region in regions:
         sync_vaults(neo4j_session, boto3_session, region, current_aws_account_id, update_tag, common_job_parameters)

    cleanup_s3_glacier(neo4j_session, common_job_parameters)







