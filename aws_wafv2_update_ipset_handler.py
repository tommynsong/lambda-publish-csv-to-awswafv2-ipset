"""Lambda handler to update AWS WAF v2 IPset from CSV in an S3 bucket."""
import os
import logging
import urllib.parse
from io import StringIO
import csv
import boto3

class AwsWafV2IpSetUpdateHandler():
    """Lambda handler to update IPSet in AWS WAF v2."""

    def __init__(self):
        """Init the AwsWafIpsetHandler."""
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        self.s3_client = boto3.client('s3')
        self.waf_client = boto3.client('wafv2')

        # These keys follow the convention defined in the CloudFormation
        # All Environment variables should use SCREAMING_SNAKE_CASE
        self.ipset_name = str(os.environ['IPSET_NAME'])
        self.ipset_id = str(os.environ['IPSET_ID'])
        self.scope = str(os.environ['SCOPE'])

    def handle_request(self, event, context):
        """Converting CSV documents into IPSet list"""
        self.logger.info(event)
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(
            event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        obj = self.s3_client.get_object(Bucket=bucket, Key=key)
        temp = obj['Body'].read()
        temp2 = temp.decode('utf-8')
        records = csv.DictReader(StringIO(temp2), delimiter=',')
        addresses = []
        ipset_lock_token = ''
        for record in records:
            addresses.append(record['ipnet'])
        
        self.logger.info(f'These are the addresses {addresses}')

        try:
            get_lock_token = self.waf_client.get_ip_set(
                Name = self.ipset_name,
                Id = self.ipset_id,
                Scope = self.scope
            )
            if 'LockToken' in get_lock_token:
                ipset_lock_token = get_lock_token.get('LockToken')
                self.logger.info(f'IPSet Lock Token {ipset_lock_token}')
            else:
                self.logger.error(f'Unable to get Lock Token')
        finally:
            pass

        try:
            request = self.waf_client.update_ip_set(
                Name = self.ipset_name,
                Scope = self.scope,
                Id = self.ipset_id,
                Addresses = addresses,
                LockToken = ipset_lock_token
            )
            if 'NextLockToken' in request:
                self.logger.info(f'Updated IPSet {self.ipset_id}')
            else:
                self.logger.error(f'Failed to update IPSet {self.ipset_id}')
        finally:
            pass
            

# Keep an instance to reuse for as long as the Lambda container lives.
HANDLER = AwsWafV2IpSetUpdateHandler()

def lambda_handler(event, context):
    """Entry point for the lambda. Invokes the handler."""
    return HANDLER.handle_request(event, context)