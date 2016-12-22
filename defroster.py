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
import os
import argparse
import tarfile
import tempfile
import hashlib
import boto3
import treehash
import logging
import datetime
import time
import math


def read_parameter(argv):
    
    parser = argparse.ArgumentParser(description='Download an archive from Amazon Glacier and unpacks it into a folder.')
    parser.add_argument('-a', '--vault', default='Photos', help='the name of the vault the archive is uploaded to')
    parser.add_argument('-d', '--debug', action='store_true', default=False, help='if debugging and info messages should be shown (default=no)')
    parser.add_argument('-i', '--info', action='store_true', default=False, help='if info messages should be shown (default=no)')
    parser.add_argument('archive_id', type=str, help='the archive ID to download')
    parser.add_argument('folder', type=str, help='the folder to download and unpack into')
    args = parser.parse_args()
    
    return args


def start_retrieval_job(glacier_client, vault_name, archive_id):
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    response = glacier_client.initiate_job(
        vaultName = vault_name,
        jobParameters = {
            #'Format': 'string',
            'Type': 'archive-retrieval',
            'ArchiveId': archive_id,
            'Description': 'Archive retrieval at %s' % ts,
            #'SNSTopic': 'string',
            #'RetrievalByteRange': 'string',
            'Tier': 'Bulk',
        }
    )
    
    return response['jobId'], response['SHA256TreeHash']


def monitor_job(glacier_client, vault_name, job_id):
    SLEEP = 60*60 # check every hour if job is completed
    
    while True:
        response = glacier_client.list_jobs(
            vaultName = vault_name,
            #limit='string',
            #marker='string',
            #statuscode='string',
            completed = 'true'
        )
        
        for job in response['JobList']:
            if job['JobId'] == job_id and job['Completed']:
                return job['RetrievalByteRange']
            else:
                end = time.localtime(time.time() + SLEEP)
                logging.info('Job is not completed yet, going back to sleep until %s' % time.strftime('%H:%M', end))
                time.sleep(SLEEP)


def download_archive(glacier_client, vault_name, job_id, byte_range, tree_hash_hex):
    SIZE_LIMIT = 100*1024*1024       # 100 MB is the recommendation
    JUNK_SIZE = 16*1024*1024         # must be a power of 2 and > 1MB
    
    (start, end) = byte_range.split('-')
    size = int(end)
    
    tar_file_name = tempfile.mktemp('.tar')
    
    if size > SIZE_LIMIT:
        logging.info('Downloading in junks')
        begin = 0
        end = 0
        f = open(tar_file_name, 'w+')
        while True:
            end = begin + JUNK_SIZE
            
            # if the last junk only request to the end
            if end > (size - 1):
                end = size - 1
            
            range = 'bytes=%i-%i' % (begin, end-1)
            response = glacier_client.get_job_output(
                vaultName = vault_name,
                jobId = job_id,
                range = range
            )
            
            # write junk to file
            body = response['body']
            while True:
                data = body.read(1024*1024)
                if data:
                    f.write(data)
                else:
                    # stop loop if there is no more data left
                    break
            
            # stop loop if download completed
            if end + 1 == size:
                break
        
        f.close()
        # TODO
    else:
        logging.info('Downloading as one, writing to: %s' % tar_file_name)
        response = glacier_client.get_job_output(
            vaultName = vault_name,
            jobId = job_id,
            #range = 'string'
        )
        
        # write stream to file
        body = response['body']
        f = open(tar_file_name, 'w+')
        while True:
            data = body.read(1024*1024)
            if data:
                f.write(data)
            else:
                break
        f.close()
        
    # check checksum
    tree_hash = calc_hash(tar_file_name)
    if tree_hash.hexdigest() != tree_hash_hex:
        logging.error('SHA256TreeHash mismatch, download failed!')
        sys.exit(2)
        
    return tar_file_name


def calc_hash(filename):
    BUF_SIZE = 1024**2
    tree_hash = treehash.TreeHash()     # default is SHA-256 and 1 MB
    
    with open(filename, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            tree_hash.update(data)
    
    return tree_hash


def unpack_tar_file(tar_file_name, output_folder):
    tar_file = tarfile.open(tar_file_name, mode='r|*')
    tar_file.extractall(output_folder)
    tar_file.close()


def delete_temp_file(filename):
    os.remove(filename)


def get_file_size(filename):
    return os.path.getsize(filename)


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
    
    logging.info('Archive ID: %s' % args.archive_id)
    
    glacier_client = boto3.client('glacier')
    #job_id, tree_hash_hex = start_retrieval_job(glacier_client, args.vault, args.archive_id)
    # 190 MB job
    job_id = 'p1Uv1IN7BQaveZLr843hheHQ1rrF47k37HscMl038m_TPtKMlyWFuXGTVc4A_o2Gknqp0wqCFA0sfLjaLHineXHkkBgb'
    tree_hash_hex = '37436496846009fd0c40fac72db10ff61457074ee4af730ef5a3abb6a06a367b'
    # 3 MB job
    #job_id = 'hwz0r-BfGXJ69ks5iC6PPA4AnAEVpd222G1mGOUMaG1OuP0qO8oy_iqq7mQxwOzL7Qpz5FlPhd6eVF4Tx0MMdtM4X5be'
    #tree_hash_hex = '3b0a8b676f33708ef577838a68cf1a1c591c981af1f9d03a50a568af79c4965d'
    logging.info('Job is created: %s' % job_id)    
    
    byte_range = monitor_job(glacier_client, args.vault, job_id)
    #byte_range = '0-3194879'
    logging.info('Job is completed. Byte range: %s' % byte_range)
    
    tar_file_name = download_archive(glacier_client, args.vault, job_id, byte_range, tree_hash_hex)
    #tar_file_name = '/var/folders/f2/jncgl3d13hd_lpx5x9290gmw0000gn/T/tmpToNSsp.tar'
    size = get_file_size(tar_file_name)
    logging.info('Downloaded TAR file: %s (%s)' % (tar_file_name, sizeof_fmt(size)))
    
    unpack_tar_file(tar_file_name, args.folder)
    logging.info('Unpacket TAR file to: %s', args.folder)
    
    delete_temp_file(tar_file_name)
    logging.info('Removed temporary file')


if __name__ == "__main__":
   main(sys.argv[1:])