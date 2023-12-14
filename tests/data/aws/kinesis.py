from datetime import datetime, timezone
import json

LIST_DATA_STREAM ={
    'StreamSummaries': [
        {
            'ResourcePolicy': json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "StreamEFOReadStatementID",
                        "Effect": "Allow",
                        "Principal": {"AWS": "arn:aws:iam::672373165745:root"},
                        "Action": ["kinesis:DescribeStreamSummary", "kinesis:ListShards"],
                        "Resource": "arn:aws:kinesis:us-east-1:672373165745:stream/test-data-stream-1"
                    }
                ]
            }),
            'StreamARN': 'arn:aws:kinesis:us-east-1:672373165745:stream/test-data-stream-1',
            'StreamCreationTimestamp': datetime(2023, 12, 10, 22, 37, 27, tzinfo=timezone.utc),
            'StreamModeDetails': {'StreamMode': 'PROVISIONED'},
            'StreamName': 'test-data-stream',
            'StreamStatus': 'ACTIVE'
        },
        {
            'ResourcePolicy': json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "StreamEFOReadStatementID",
                        "Effect": "Allow",
                        "Principal": {"AWS": "arn:aws:iam::672373165745:root"},
                        "Action": ["kinesis:DescribeStreamSummary", "kinesis:ListShards"],
                        "Resource": "arn:aws:kinesis:us-east-1:672373165745:stream/test-data-stream-2"
                    }
                ]
             }),
            'StreamARN': 'arn:aws:kinesis:us-east-1:672373165745:stream/test-data-stream-2',
            'StreamCreationTimestamp': datetime(2023, 12, 11, 10, 15, 45, tzinfo=timezone.utc),
            'StreamModeDetails': {'StreamMode': 'PROVISIONED'},
            'StreamName': 'test-data-stream-2',
            'StreamStatus': 'ACTIVE'
        },
        {
            'ResourcePolicy': 'No resource policy to display',
            'StreamARN': 'arn:aws:kinesis:us-east-1:672373165745:stream/test-data-stream-3',
            'StreamCreationTimestamp': datetime(2023, 12, 12, 18, 42, 30, tzinfo=timezone.utc),
            'StreamModeDetails': {'StreamMode': 'PROVISIONED'},
            'StreamName': 'test-data-stream-3',
            'StreamStatus': 'ACTIVE'

        }
    ]
}

LIST_VIDEO_STREAM = {
    'StreamInfoList': [
        {
            'DeviceName': 'Camera1',
            'StreamName': 'test-video-stream-1',
            'StreamARN': 'arn:aws:kinesisvideo:us-east-1:672373165745:stream/test-video-stream-1/1702497297923',
            'MediaType': 'video',
            'KmsKeyId': 'arn:aws:kms:us-east-1:672373165745:alias/aws/kinesisvideo',
            'Version': 'ZSxB8yy4P2Rjm9wscchQ',
            'Status': 'ACTIVE',
            'CreationTime': datetime(2023, 12, 14, 1, 24, 57, 924000, tzinfo=timezone.utc),
            'DataRetentionInHours': 24
        },
        {
            'DeviceName': 'Camera2',
            'StreamName': 'test-video-stream-2',
            'StreamARN': 'arn:aws:kinesisvideo:us-east-1:672373165745:stream/test-video-stream-2/1702497297924',
            'MediaType': 'video',
            'KmsKeyId': 'arn:aws:kms:us-east-1:672373165745:alias/aws/kinesisvideo',
            'Version': 'ZSxB8yy4P2Rjm9wscchQ',
            'Status': 'ACTIVE',
            'CreationTime': datetime(2023, 12, 14, 1, 24, 57, 924000, tzinfo=timezone.utc),
            'DataRetentionInHours': 24
        },
        {
            'DeviceName': 'Camera3',
            'StreamName': 'test-video-stream-3',
            'StreamARN': 'arn:aws:kinesisvideo:us-east-1:672373165745:stream/test-video-stream-3/1702497297925',
            'MediaType': 'video',
            'KmsKeyId': 'arn:aws:kms:us-east-1:672373165745:alias/aws/kinesisvideo',
            'Version': 'ZSxB8yy4P2Rjm9wscchQ',
            'Status': 'ACTIVE',
            'CreationTime': datetime(2023, 12, 14, 1, 24, 57, 924000, tzinfo=timezone.utc),
            'DataRetentionInHours': 24
        }
    ]
}
