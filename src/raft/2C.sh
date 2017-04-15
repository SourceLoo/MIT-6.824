#!/bin/bash 

path="/Users/source/WorkSpace/myGit/mine/6.824"
export "GOPATH=$path"
`cd /Users/source/WorkSpace/myGit/mine/6.824/src/raft`
out=2C

for (( i = 1; i > 0; i++ )); do
    `rm $out`
    echo $i
    echo $i >> $out
    #txtA=`go test -run 2A -race 2>&1`
    #txtB=`go test -run 2B -race 2>&1`
    txtC=`go test -run $out 2>&1`

    #txt=$txtA$txtB
    txt=$txtC

    if  echo $txt | grep -q FAIL; then
        echo "${txt}" >> $out
        exit 0
    fi
done
