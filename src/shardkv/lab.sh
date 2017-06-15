#!/usr/bin/env bash
export GOPATH=/Users/source/WorkSpace/myGit/mine/6.824
while true; do echo $((i=$i+1)) >> logcnt; go test > log 2>&1 || break; done
