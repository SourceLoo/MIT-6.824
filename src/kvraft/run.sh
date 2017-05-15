#name=TestSnapshotUnreliableRecoverConcurrentPartition
#name=TestPersistPartitionUnreliable
#name=TestPersist
#name=TestSnapshotRPC
#name=TestSnapshotRecover
#name=TestSnapshotUnreliable
name=Test

go test -timeout 40m -run $name > log00 2>&1 &
go test -timeout 40m -run $name > log01 2>&1 &
go test -timeout 40m -run $name > log02 2>&1 &
go test -timeout 40m -run $name > log03 2>&1 &
go test -timeout 40m -run $name > log04 2>&1 &
go test -timeout 40m -run $name > log05 2>&1 &
go test -timeout 40m -run $name > log06 2>&1 &
go test -timeout 40m -run $name > log07 2>&1 &
go test -timeout 40m -run $name > log08 2>&1 &
go test -timeout 40m -run $name > log09 2>&1 &
go test -timeout 40m -run $name > log10 2>&1 &
go test -timeout 40m -run $name > log11 2>&1 &
go test -timeout 40m -run $name > log12 2>&1 &
go test -timeout 40m -run $name > log13 2>&1 &
go test -timeout 40m -run $name > log14 2>&1 &
go test -timeout 40m -run $name > log15 2>&1 &
go test -timeout 40m -run $name > log16 2>&1 &
go test -timeout 40m -run $name > log17 2>&1 &
go test -timeout 40m -run $name > log18 2>&1 &
go test -timeout 40m -run $name > log19 2>&1 &
