import datetime
import json

LIST_VAULT = {
    'VaultList': [
        {
            'VaultARN': 'arn:aws:glacier:us-east-1:672373165745:vaults/vault-1',
            'VaultName': 'vault-1',
            'CreationDate': datetime.datetime(2023, 12, 1, 20, 18, 58, 666000).isoformat() + 'Z',
            'LastInventoryDate': datetime.datetime(2023, 12, 4, 16, 46, 25, 829000).isoformat() + 'Z',
            'NumberOfArchives': 17,
            'SizeInBytes': 966161,
            'AccessPolicy': json.dumps({
                'Policy': {
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Principal': '*',
                            'Action': 'glacier:*',
                            'Resource': 'arn:aws:glacier:us-east-1:672373165745:vaults/vault-1',
                        }
                    ]
                }
            })
        },
        {
            'VaultARN': 'arn:aws:glacier:us-east-1:672373165745:vaults/vault-2',
            'VaultName': 'vault-2',
            'CreationDate': datetime.datetime(2023, 12, 2, 10, 30, 45, 123000).isoformat() + 'Z',
            'LastInventoryDate': datetime.datetime(2023, 12, 5, 12, 15, 10, 456000).isoformat() + 'Z',
            'NumberOfArchives': 23,
            'SizeInBytes': 1245678,
             'AccessPolicy': json.dumps({
                'Policy': {
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Principal': '*',
                            'Action': 'glacier:*',
                            'Resource': 'arn:aws:glacier:us-east-1:672373165745:vaults/vault-2',
                        }
                    ]
                }
            })
        },
        {
            'VaultARN': 'arn:aws:glacier:us-east-1:672373165745:vaults/vault-3',
            'VaultName': 'vault-3',
            'CreationDate': datetime.datetime(2023, 12, 3, 15, 42, 30, 987000).isoformat() + 'Z',
            'LastInventoryDate': datetime.datetime(2023, 12, 6, 8, 5, 55, 321000).isoformat() + 'Z',
            'NumberOfArchives': 8,
            'SizeInBytes': 543210,
            'AccessPolicy': 'No vault access policy to display'
        },
    ]
}
