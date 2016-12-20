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

# TODO
# add compression option
# add upload/download/list options

import sys
import os
import argparse
import tarfile
import tempfile
import hashlib
import boto3
import treehash
import logging


def read_parameter(argv):
    
    parser = argparse.ArgumentParser(description='Uploads a folder as a TAR archive to Amazon Glacier.')
    parser.add_argument('-a', '--vault', default='Photos', help='the name of the vault the archive is uploaded to')
    parser.add_argument('-d', '--debug', action='store_true', default=False)
    parser.add_argument('folder', type=str, help='the folder to upload')
    args = parser.parse_args()
    
    if not os.path.isdir(args.folder):
        print "folder %s is not a directory or does not exist" % args.folder
        sys.exit(2)
    
    return args


def create_tar(input_folder):
    tar_file_name = tempfile.mktemp('.tar')
    tar_file = tarfile.open(tar_file_name, mode='w')
    tar_file.add(input_folder, arcname='')
    tar_file.close()
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


def upload_to_glacier(filename, description, vault_name, tree_hash):
    glacier_client = boto3.client('glacier')
    
    logging.info("Uploading: %s" % filename)
    
    res = glacier_client.upload_archive(
        vaultName=vault_name, 
        archiveDescription=description,
        body=open(filename, 'rb'),
        checksum=tree_hash
    )
    
    #res = {}
    #res['checksum'] = tree_hash
    #res['archiveId'] = 'test-test'
    if tree_hash != res['checksum']:
        logging.error("Checksum mismatch: %s" % res['checksum'])
        sys.exit(2)
    
    return res['archiveId']


def delete_temp_file(filename):
    os.remove(filename)


def main(argv):
    args = read_parameter(argv)
    
    if args.debug:
        level = logging.DEBUG
        logging.basicConfig(level=logging.DEBUG,format="%(levelname)s: %(message)s")
    else:
        logging.basicConfig(level=logging.WARNING,format="%(message)s")
    
    logging.info('Input folder:            %s' % args.folder)
    
    tar_file = create_tar(args.folder)
    logging.info('Created TAR file:        %s' % tar_file)
    
    tree_hash = calc_hash(tar_file)
    logging.info('Hash (SHA-256 treehash): %s' % tree_hash.hexdigest())
    
    description = "files from %{input_folder}, SHA-256 treehash %{tree_hash.hexdigest()}"
    archive_id = upload_to_glacier(tar_file, description, args.vault, tree_hash.hexdigest())
    
    delete_temp_file(tar_file)
    logging.info('Removed temporary file')
    
    print "%s\t%s\t%s" % (args.folder, args.vault, archive_id)


if __name__ == "__main__":
   main(sys.argv[1:])