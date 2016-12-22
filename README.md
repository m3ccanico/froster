# Introduction

Froster is a collection of three scripts to upload, download, or list data from Amazon Glacier.

The script `froster.py` creates a TAR file of all files within a folder, optionally compress it, and uploads it to Amazon Glacer.

The script `list.py` lists all archives in a vault on Amazon Glacier.

The script `defroster.py` downloads an archive from a vault and unpacks the TAR file into a specific folder.

# Installation

Download the scripts as a ZIP file or clone them with git:
```bash
git clone https://github.com/m3ccanico/froster.git
```

You'll need Python to be installed on you system and you might need to install additional Pyhton modules:
```bash
pip install boto3 treehash
```

Test them with
```bash
./list.php -h
```

# Configuration

## AWS

You'll need to have a AWS account (https://aws.amazon.com/) to run these scripts. In AWS you'll need to
1. Create a Galcier Vault (e.g. Photos)
2. Create an user (e.g. frosty) for the script under the IAM. This will give you an *access key ID* and ab *secret access key*.
3. Give the user the permiss *AmazonGlacierFullAccess*

## On your system

Create the file `~/.aws/config` and add the follwing text:
```text
[default]
region=ap-southeast-2
```

You might need to adjust the region based on your requirements. Typically you want to use the region closest to you for the best performance. Although, it might make sense to chose another region in case you are worried a natural disaster might affect your local copy of the data as well as the copy you upload to Amazon. You find the other region strings under the section *Amazon API Gateway* in http://docs.aws.amazon.com/general/latest/gr/rande.html.

Create the file `~/.aws/credentials` and add the follwing text:
```text
[default]
aws_access_key_id=SECRETSECRETSECRETSE
aws_secret_access_key=0123456789secret0123456789secret01234567
```
Obviously you'll need to copy/paste the *access key ID* and the *secret access key* from the AWS web interface.

# Usage

Either give the scripts the execute permission or run them with the python interpreter.

Amazon Glacier is a relatively slow services. The inventory list and downloading are asynchronous jobs that can take hours to complete. The script submits the jobs and then monitors the job until it is completed. By default, it checks the job every hour (60min). It typically doesn't make sense to check the job more often.

Upload the folder `~/Photos/2012` into the vault `Photos` with some verbose output:
```bash
./froster.py --info --vault Photos ~/Photos/2012
```

List the uploaded archives:
```bash
./list.py --info--vault Photos
```

Download the archive `xpZ...` from the vault Photos and unpack it into the folder `~/tmp`:
```bash
./defroster.py --info --vault Photos \
  xpZrRLqP7YSrG4xKvhI6C-eUlHrC2crbHFhygIVPTdufwH9tyH1kIo_ZxSZyt-WLANc-O-38wOuXppQAzoMH8vkEbfj5lbhu4SvIfXx9WJjyRSKmgabcLycxjl2KUDG1NKVvoQlYAQ \
  ~/tmp
```