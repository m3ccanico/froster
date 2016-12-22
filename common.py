import sys
import os
import treehash
import time
import logging
import boto3

def monitor_job(glacier_client, vault_name, job_id, timeout):
    SLEEP = int(timeout)*60 # check every 20min if job is completed
    
    while True:
        response = glacier_client.list_jobs(
            vaultName = vault_name,
            #completed = 'true'
        )
        
        found = False
        completed = False
        
        for job in response['JobList']:
            #pp = pprint.PrettyPrinter(indent=4)
            #pp.pprint(job)
            if job['JobId'] == job_id:
                found = True
                completed = job['Completed']
                break
        
        if found and completed:
            return int(job['ArchiveSizeInBytes'])
        
        if found:
            end = time.localtime(time.time() + SLEEP)
            logging.info('Job is not completed yet, going back to sleep until %s' % time.strftime('%H:%M', end))
            time.sleep(SLEEP)
        else: 
            logging.error('Job not found: %s' % job_id)
            sys.exit(2)


def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def delete_temp_file(filename):
    os.remove(filename)


def get_file_size(filename):
    return os.path.getsize(filename)


def get_tree_hash_of_file(filename):
    BUF_SIZE = 1024**2
    tree_hash = treehash.TreeHash()     # default is SHA-256 and 1 MB
    
    with open(filename, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            tree_hash.update(data)
    
    return tree_hash.hexdump()