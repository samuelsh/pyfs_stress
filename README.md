# pyFstress
Multi-client file system load and stress testing tool based on ZeroMQ.
It can be useful for anyone, who is developing new file system or testing an existing one.

Use cases:
1. Load & Stress - Imagine 60 clients, each one running 16 io processes vs your file server :)
2. Race Conditions - Client x trying to access file which is removed by client y, while client z reading it
3. Data Corruptions - Controller always "knows" the current status of all files and directories

Features:
* Multi-client load & stress - tested on 60 clients simultaneously ( and it's still not the limit :))
* Data integrity & corruptions monitoring - able to detect lost files or file chunks, containing wrong data
* NFS and SMB mounts are supported
* Following file system operations supported: mkdir, list, remove, touch, stat, read, rename, move,
  write, truncate
* Random and sequential reads and writes supported
* NFS advisory file locking
* Centralised logging (PUB - SUB) for all clients
* Configurable workloads (JSON)
* Flexible configuration (JSON)

Linux distributions tested: CentOS 6, CentOS 7, Debian Jessy, Ubuntu 14.04

System Requirements:
* File Server under test should support nfs or smb protocol
* At least 2 physical machines or VMs for Sever (controller) and one client
* At least 8GB RAM for server (controller) machine
* At least 4GB RAM for client machine

Required Python packages:
* pyzmq
* paramiko
* treelib
* argparse

Usage:
```bash
$ ./fileops_server.py -h
usage: fileops_server.py [-h] [-e EXPORT] [--tenants]
                         [-m {nfs3,nfs4,nfs4.1,smb1,smb2,smb3}]
                         cluster clients [clients ...]

pyFstress Server runner

positional arguments:
  cluster               File server name
  clients               Space separated list of clients

optional arguments:
  -h, --help            show this help message and exit
  -e EXPORT, --export EXPORT
                        NFS export name
  --tenants             Enable MultiTenancy
  -m {nfs3,nfs4,nfs4.1,smb1,smb2,smb3}, --mtype {nfs3,nfs4,nfs4.1,smb1,smb2,smb3}
                        Mount type

```

Feature plans:
* Implement windows clients support
* Improve data integrity check
* Performance stats
* Additional workloads
* Performance optimisation