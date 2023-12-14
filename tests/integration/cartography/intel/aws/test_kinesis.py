from pprint import pprint

import cartography.intel.aws.kinesis
import tests.data.aws.kinesis

TEST_ACCOUNT_ID = '000000000000'
TEST_REGION = 'us-east-1'
TEST_UPDATE_TAG = 123456789

def test_load_list_of_data_streams(neo4j_session, *args):
    """
    Ensure that expected data streams get loaded with their key fields.
    """
    data = tests.data.aws.kinesis.LIST_DATA_STREAM['StreamSummaries']
    cartography.intel.aws.kinesis.load_list_of_data_streams(
        neo4j_session, data, TEST_REGION, TEST_ACCOUNT_ID, TEST_UPDATE_TAG,
    )

    expected_data_streams = {"test-data-stream", "test-data-stream-2", "test-data-stream-3"}

    nodes = neo4j_session.run(
        """
        MATCH (d:KinesisDataStream) RETURN d.streamName
        """,
    )
    actual_data_streams = {record['d.streamName'] for record in nodes}

    # pprint(actual_data_streams)

    assert actual_data_streams == expected_data_streams


def test_load_list_of_video_streams(neo4j_session, *args):
    """
    Ensure that expected video streams get loaded with their key fields.
    """
    data = tests.data.aws.kinesis.LIST_VIDEO_STREAM['StreamInfoList']
    cartography.intel.aws.kinesis.load_list_of_video_streams(
        neo4j_session, data, TEST_REGION, TEST_ACCOUNT_ID, TEST_UPDATE_TAG,
    )

    expected_video_streams = {"test-video-stream-1", "test-video-stream-2", "test-video-stream-3"}

    nodes = neo4j_session.run(
        """
        MATCH (v:KinesisVideoStream) RETURN v.streamName
        """,
    )
    actual_video_streams = {record['v.streamName'] for record in nodes}

    # pprint(actual_video_streams)

    assert actual_video_streams == expected_video_streams