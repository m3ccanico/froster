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


def read_parameter(argv):
    
    parser = argparse.ArgumentParser(description='Lists the archives in an Amazon Glacier vault.')
    parser.add_argument('-a', '--vault', default='Photos', help='the name of the vault the archive is uploaded to')
    parser.add_argument('-d', '--debug', action='store_true', default=False, help='if debugging and info messages should be shown (default=no)')
    parser.add_argument('-i', '--info', action='store_true', default=False, help='if info messages should be shown (default=no)')
    #parser.add_argument('archive_id', type=str, help='the archive ID to download')
    #parser.add_argument('folder', type=str, help='the folder to download and unpack into')
    args = parser.parse_args()
    
    return args


def start_list_job(glacier_client, vault_name):
    time = time.strftime('%Y-%m-%d %H:%M:%S', end)
    response = glacier_client.initiate_job(
        vaultName = vault_name,
        jobParameters = {
            'Type': 'inventory-retrieval',
            'Description': 'Inventory retrieval at %s' % time,
        }
    )
    return response['jobId']


def monitor_job(glacier_client, vault_name, job_id):
    SLEEP = 20*60
    
    while True:
        response = glacier_client.list_jobs(
            vaultName = vault_name,
            completed = 'true'
        )
        
        for job in response['JobList']:
            #print job
            if job['JobId'] == job_id and job['Completed']:
                return
            else:
                end = time.localtime(time.time() + SLEEP)
                logging.info('Job is not completed yet, going back to sleep until %s' % time.strftime('%H:%M', end))
                time.sleep(SLEEP)


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
        


def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def main(argv):
    args = read_parameter(argv)
    
    if args.info:
        logging.basicConfig(level=logging.INFO,format="%(levelname)s: %(message)s")
    if args.debug:
        logging.basicConfig(level=logging.DEBUG,format="%(levelname)s: %(message)s")
    else:
        logging.basicConfig(level=logging.WARNING,format="%(message)s")
    
    logging.info('List archives in %s' % args.vault)
    
    glacier_client = boto3.client('glacier')
    job_id = start_list_job(glacier_client, args.vault)
    #job_id = '37Q9FXefFtbPlOx2512mFL_hO7m5lSn66w3LiudXkMYmFfqGnxKjtxne-D3SZmdD9d3-qLJInhkZMwdPY8aJVW8_29MG'
    
    monitor_job(glacier_client, args.vault, job_id)
    logging.info('Inventory is ready')
    
    inventory = download_inventory(glacier_client, args.vault, job_id)
    logging.info('Downloaded inventory')
    
    print_inventory(inventory)


if __name__ == "__main__":
   main(sys.argv[1:])