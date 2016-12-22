# Introduction

# Installation

# Configuration

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