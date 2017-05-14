#!/bin/bash

path="/home/users/luyq/workspace/6.824"
export GOPATH=$path

cd ~/workspace/6.824/src/kvraft

out=exp
i=0
while [ "$i" -ge 0 ]; do
    i=$((i+1))
    rm $out
    echo $i
    echo $i >> $out
    txt=`go test 2>&1`
    if echo $txt | grep -q FAIL; then
         echo "${txt}" >> $out
         exit 0
     fi
done


