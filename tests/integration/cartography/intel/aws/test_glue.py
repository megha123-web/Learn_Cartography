from unittest.mock import MagicMock
from pprint import pprint
import cartography.intel.aws.glue
import tests.data.aws.glue

TEST_ACCOUNT_ID = '000000000000'
TEST_REGION = 'us-east-1'
TEST_UPDATE_TAG = 123456789

def test_load_list_of_databases(neo4j_session, *args):
    """
    Ensure that expected databases get loaded with their key fields.
    """
    data = tests.data.aws.glue.LIST_DATABASES['DatabaseList']
    boto3_session = MagicMock()
    cartography.intel.aws.glue.load_list_of_databases(
        neo4j_session, boto3_session ,data, TEST_REGION, TEST_ACCOUNT_ID, TEST_UPDATE_TAG,
    )

    expected_databases = {"customer_database", "product_database", "hr_database"}

    nodes = neo4j_session.run(
        """
        MATCH (db:GlueDatabase) RETURN db.databaseName
        """,
    )

    actual_databases = {record['db.databaseName'] for record in nodes}

    print("Actual Databases:", actual_databases)
    print("Expected Databases:", expected_databases)

    assert actual_databases == expected_databases



def test_load_list_of_tables(neo4j_session, *args):
    """
    Ensure that expected tables get loaded with their key fields.
    """
    data = tests.data.aws.glue.LIST_TABLES['TableList']
    boto3_session = MagicMock()
    cartography.intel.aws.glue.load_list_of_tables(
        neo4j_session, data, TEST_REGION, TEST_ACCOUNT_ID, TEST_UPDATE_TAG,
    )

    expected_tables = {"customer_csv"}

    nodes = neo4j_session.run(
        """
        MATCH (t:GlueTable) RETURN t.tableName
        """,
    )

    actual_tables = {record['t.tableName'] for record in nodes}

    assert actual_tables == expected_tables



def test_load_list_of_crawlers(neo4j_session, *args):
    """
    Ensure that expected crawlers get loaded with their key fields.
    """
    data = tests.data.aws.glue.LIST_CRAWLERS['Crawlers']
    cartography.intel.aws.glue.load_list_of_crawlers(
        neo4j_session, data, TEST_REGION, TEST_ACCOUNT_ID, TEST_UPDATE_TAG,
    )

    expected_crawlers = {"crawler_sales_data"}

    nodes = neo4j_session.run(
        """
        MATCH (c:GlueCrawler) RETURN c.name
        """,
    )

    actual_crawlers = {record['c.name'] for record in nodes}

    assert actual_crawlers == set(expected_crawlers)



def test_load_list_of_jobs(neo4j_session, *args):
    """
    Ensure that expected jobs get loaded with their key fields.
    """
    data = tests.data.aws.glue.LIST_JOBS['Jobs']
    cartography.intel.aws.glue.load_list_of_jobs(
        neo4j_session, data, TEST_REGION, TEST_ACCOUNT_ID, TEST_UPDATE_TAG,
    )

    expected_jobs = {"data-processing-job"}

    nodes = neo4j_session.run(
        """
        MATCH (j:GlueJob) RETURN j.name
        """,
    )

    actual_jobs = {record['j.name'] for record in nodes}

    assert actual_jobs == set(expected_jobs)

def test_load_list_of_connections(neo4j_session, *args):
    """
    Ensure that expected connections get loaded with their key fields.
    """
    data = tests.data.aws.glue.LIST_CONNECTIONS['ConnectionList']
    cartography.intel.aws.glue.load_list_of_connections(
        neo4j_session, data, TEST_REGION, TEST_ACCOUNT_ID, TEST_UPDATE_TAG,
    )

    expected_connections = {"S3 Connection", "Redshift Connection"}

    nodes = neo4j_session.run(
        """
        MATCH (c:GlueConnection) RETURN c.name
        """,
    )

    actual_connections = {record['c.name'] for record in nodes}

    assert actual_connections == set(expected_connections)

def test_load_list_of_interactive_sessions(neo4j_session, *args):
    """
    Ensure that expected sessions get loaded with their key fields.
    """
    data = tests.data.aws.glue.LIST_INTERACTIVE_SESSIONS['Sessions']
    cartography.intel.aws.glue.load_list_of_interactive_sessions(
        neo4j_session, data, TEST_REGION, TEST_ACCOUNT_ID, TEST_UPDATE_TAG,
    )

    expected_sessions = {"gluescript", "pyspark"}

    nodes = neo4j_session.run(
        """
        MATCH (s:GlueInteractiveSession) RETURN s.name
        """,
    )

    actual_sessions = {record['s.name'] for record in nodes}

    assert actual_sessions == set(expected_sessions)