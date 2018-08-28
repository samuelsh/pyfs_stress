#!/usr/bin/env bash

clients="$1"

function fatal() {
        echo $*
        exit 1
}

for client in $clients; do
    echo "Preparing client ${client}"
    ssh vastdata@${client} "sudo yum -y install https://centos7.iuscommunity.org/ius-release.rpm"
    ssh vastdata@${client} "sudo yum -y install python36u"
    ssh vastdata@${client} "sudo yum -y install python36u-pip"
    ssh vastdata@${client} "sudo pip3.6 install zmq" || fatal "Failed to install package - zmq"
    ssh vastdata@${client} "sudo pip3.6 install redis" ||  fatal "Failed to install package - redis"
    ssh vastdata@${client} "sudo pip3.6 install xxhash" || fatal "Failed to install package - xxhash"
done

exit 0