from pprint import pprint

import cartography.intel.aws.glacier
import tests.data.aws.glacier

TEST_ACCOUNT_ID = '000000000000'
TEST_REGION = 'us-east-1'
TEST_UPDATE_TAG = 123456789


def test_load_list_of_vaults(neo4j_session, *args):
    """
    Ensure that expected vaults get loaded with their key fields.
    """
    data = tests.data.aws.glacier.LIST_VAULT['VaultList']
    cartography.intel.aws.glacier.load_list_of_vaults(
        neo4j_session, data, TEST_REGION, TEST_ACCOUNT_ID, TEST_UPDATE_TAG,
    )

    expected_vaults = {"vault-1", "vault-2", "vault-3"}

    nodes = neo4j_session.run(
        """
        MATCH (v:S3GlacierVault) RETURN v.vaultname
        """,
    )
    actual_vaults = {record['v.vaultname'] for record in nodes}

    pprint(actual_vaults)

    assert actual_vaults == expected_vaults
