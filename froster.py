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
import math
import re

def read_parameter(argv):
    
    parser = argparse.ArgumentParser(description='Uploads a folder as a TAR archive to Amazon Glacier.')
    parser.add_argument('-a', '--vault', default='Photos', help='the name of the vault the archive is uploaded to')
    parser.add_argument('-c', '--compress', action='store_true', default=False, help='if the archive should be compressed with bzip2 (default=no)')
    parser.add_argument('-d', '--debug', action='store_true', default=False, help='if debugging and info messages should be shown (default=no)')
    parser.add_argument('-i', '--info', action='store_true', default=False, help='if info messages should be shown (default=no)')
    parser.add_argument('folder', type=str, help='the folder to upload')
    args = parser.parse_args()
    
    if not os.path.isdir(args.folder):
        print "folder %s is not a directory or does not exist" % args.folder
        sys.exit(2)
    
    return args


def create_tar(input_folder, compress):
    if compress:
        tar_file_name = tempfile.mktemp('.tar.bz2')
        tar_file = tarfile.open(tar_file_name, mode='w:bz2')
    else:
        tar_file_name = tempfile.mktemp('.tar')
        tar_file = tarfile.open(tar_file_name, mode='w')
    tar_file.add(input_folder, arcname='')
    tar_file.close()
    return tar_file_name


def get_file_size(filename):
    return os.path.getsize(filename)


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


def upload_to_glacier(filename, size, description, vault_name, tree_hash):
    SIZE_LIMIT = 100*1024*1024       # 100 MB is the recommendation
    CHUNK_SIZE = 16*1024*1024        # must be a power of 2
    
    glacier_client = boto3.client('glacier')
    
    if size > SIZE_LIMIT:
        logging.info("Uploading multipart:     %s" % filename)
        
        # inform AWS that we want to upload chunks
        response = glacier_client.initiate_multipart_upload(
            vaultName=vault_name,
            archiveDescription=description,
            partSize="%i" % CHUNK_SIZE
        )
        upload_id = response['uploadId']
        
        # start uploading chunks to AWS
        begin = 0
        end = 0
        cnt = 1
        total = math.ceil(1.0*size / CHUNK_SIZE)
        f = open(filename, 'rb')
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            end = begin + len(chunk)
            chunk_hash = treehash.TreeHash()
            chunk_hash.update(chunk)
            
            range = "bytes %i-%i/*" % (begin, end-1)
            
            logging.info(" uploading chunk:         %i of %i (%i-%i)" % (cnt, total, begin, end-1))
            response = glacier_client.upload_multipart_part(
                vaultName=vault_name,
                body=chunk,
                range=range,
                checksum=chunk_hash.hexdigest(),
                uploadId=upload_id
            )
            
            if chunk_hash.hexdigest() != response['checksum']:
                logging.error("Checksum mismatch in chunk: %s" % response['checksum'])
                sys.exit(2)
            
            begin = end
            cnt += 1
        
        # inform AWS that we're done
        response = glacier_client.complete_multipart_upload(
            vaultName=vault_name,
            uploadId=upload_id,
            archiveSize=str(size),
            checksum=tree_hash
        )
        
        if tree_hash != response['checksum']:
            logging.error("Checksum mismatch: %s" % response['checksum'])
            sys.exit(2)
        archive_id = response['archiveId']
        
    else:
        logging.info("Uploading:               %s" % filename)
        response = glacier_client.upload_archive(
            vaultName=vault_name, 
            archiveDescription=description,
            body=open(filename, 'rb'),
            checksum=tree_hash
        )
        
        #response = {}
        #response['checksum'] = tree_hash
        #response['archiveId'] = 'test-test'
        if tree_hash != response['checksum']:
            logging.error("Checksum mismatch: %s" % response['checksum'])
            sys.exit(2)
        
        archive_id = response['archiveId']
        
    return archive_id


def delete_temp_file(filename):
    os.remove(filename)


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
    
    logging.info('Input folder:            %s' % args.folder)
    
    tar_file = create_tar(args.folder, args.compress)
    logging.info('Created TAR file:        %s' % tar_file)
    
    size = get_file_size(tar_file)    
    logging.info('File size:               %s (%i bytes)' % (sizeof_fmt(size), size))
    
    tree_hash = calc_hash(tar_file)
    logging.info('Hash (SHA-256 treehash): %s' % tree_hash.hexdigest())
    
    description = "files from %s" % re.sub(r'[^\x00-\x7F]+','', args.folder)    # remove non ASCII chars
    archive_id = upload_to_glacier(tar_file, size, description, args.vault, tree_hash.hexdigest())
    
    delete_temp_file(tar_file)
    logging.info('Removed temporary file')
    
    now = datetime.datetime.now()
    
    print "%s\t%s\t%s\t%s\t%s" % (now, args.folder, args.vault, archive_id, tree_hash.hexdigest())


if __name__ == "__main__":
   main(sys.argv[1:])