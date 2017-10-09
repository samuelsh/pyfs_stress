# MultiFileOps
Multi-client file system load and stress testing tool based on ZeroMQ

Usage:
```bash
$ ./fileops_server.py -h
usage: fileops_server.py [-h] -c CLUSTER --clients CLIENTS [CLIENTS ...]
                         [-e EXPORT] [--tenants]
                         [-m {nfs3,nfs4,nfs4.1,smb1,smb2,smb3}]

FileOps Server starter - 2016 samuels(c)

optional arguments:
  -h, --help            show this help message and exit
  -c CLUSTER, --cluster CLUSTER
                        Cluster name
  --clients CLIENTS [CLIENTS ...]
                        Space separated list of clients
  -e EXPORT, --export EXPORT
                        Space separated list of clients
  --tenants             Enable MultiTenancy
  -m {nfs3,nfs4,nfs4.1,smb1,smb2,smb3}, --mtype {nfs3,nfs4,nfs4.1,smb1,smb2,smb3}
                        Mount type
```
