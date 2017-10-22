#!/usr/bin/env python


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
    
    parser = argparse.ArgumentParser(description='Delete san archives in an Amazon Glacier vault.')
    parser.add_argument('-a', '--vault', default='Photos', help='the name of the vault the archive is deleted from')
    parser.add_argument('-d', '--debug', action='store_true', default=False, help='if debugging and info messages should be shown (default=no)')
    parser.add_argument('-i', '--info', action='store_true', default=False, help='if info messages should be shown (default=no)')
    parser.add_argument('archive_id', type=str, help='the archive ID to delete')
    args = parser.parse_args()
    
    return args



def main(argv):
    args = read_parameter(argv)
    
    if args.info:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")
    else:
        logging.basicConfig(level=logging.WARNING, format="%(message)s")
    
    logging.info('Deletes archive %s from vault %s' % (args.archive_id, args.vault))
    
    glacier_client = boto3.client('glacier')
    response = glacier_client.delete_archive(vaultName=args.vault, archiveId=args.archive_id)
    
    print response


if __name__ == "__main__":
   main(sys.argv[1:])