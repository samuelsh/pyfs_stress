# pyFstress
Multi-client file system load and stress testing tool based on ZeroMQ

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
