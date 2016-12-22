#!/usr/bin/env python

# pip install boto3 treehash
#
# AWS credentials in ~/.aws/credentials:
# [default]
# aws_access_key_id=secret
# aws_secret_access_key=secret
#
# AWS region in ~/aws/settings:
# [default]
# region=ap-southeast-2


import sys
import argparse
import boto3
import logging
import datetime
import time
import json

from common import monitor_job
from common import sizeof_fmt


def read_parameter(argv):
    
    parser = argparse.ArgumentParser(description='Lists the archives in an Amazon Glacier vault.')
    parser.add_argument('-a', '--vault', default='Photos', help='the name of the vault the archive is uploaded to')
    parser.add_argument('-d', '--debug', action='store_true', default=False, help='if debugging and info messages should be shown (default=no)')
    parser.add_argument('-i', '--info', action='store_true', default=False, help='if info messages should be shown (default=no)')
    parser.add_argument('-t', '--timeout', default=60, help='the number of minutes to wait between job status checks (default=60)')
    args = parser.parse_args()
    
    return args


def start_list_job(glacier_client, vault_name):
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    response = glacier_client.initiate_job(
        vaultName = vault_name,
        jobParameters = {
            'Type': 'inventory-retrieval',
            'Description': 'Inventory retrieval at %s' % ts,
        }
    )
    return response['jobId']


def download_inventory(glacier_client, vault_name, job_id):
    response = glacier_client.get_job_output(
        vaultName = vault_name,
        jobId = job_id,
    )
    body = response['body']
    return json.loads(body.read())


def print_inventory(inventory):
    print 'Date:',  inventory['InventoryDate']
    for archive in inventory['ArchiveList']:
        print '-'
        print 'ID:            ', archive['ArchiveId']
        print 'Description:   ', archive['ArchiveDescription']
        print 'Creation date: ', archive['CreationDate']
        print 'Size:          ', sizeof_fmt(archive['Size']), '(%i)' % archive['Size']
        print 'SHA256TreeHash:', archive['SHA256TreeHash']
        


def main(argv):
    args = read_parameter(argv)
    
    if args.info:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")
    else:
        logging.basicConfig(level=logging.WARNING, format="%(message)s")
    
    logging.info('List archives in %s' % args.vault)
    
    glacier_client = boto3.client('glacier')
    job_id = start_list_job(glacier_client, args.vault)
    #job_id = 'OtXy6xs7IXyCQKz7oLy0i1PVN-Ym-hJgxI9osCCJ3ZBE0BjRGfHqwWry2sj_c4UcghPNxTn7spF6cU1beronoNNCamjT'
    
    monitor_job(glacier_client, args.vault, job_id, args.timeout)
    logging.info('Inventory is ready')
    
    inventory = download_inventory(glacier_client, args.vault, job_id)
    logging.info('Downloaded inventory')
    
    print_inventory(inventory)


if __name__ == "__main__":
   main(sys.argv[1:])