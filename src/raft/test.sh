#!/bin/bash 

path="/Users/source/WorkSpace/myGit/mine/6.824"
export "GOPATH=$path"
`cd /Users/source/WorkSpace/myGit/mine/6.824/src/raft`
`rm out.txt`
for (( i = 1; i > 0; i++ )); do
    echo $i
    echo $i >> out.txt
    `go test -run TestRejoin2B -race >> out.txt 2>&1`
done

