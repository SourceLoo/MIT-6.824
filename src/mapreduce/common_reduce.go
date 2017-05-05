package mapreduce

import (
	"encoding/json"
	"os"
	"log"
	"sort"
)

// 增加代码 步骤2的排序
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key } // 按key string 排序


// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	// 1. 获得所有文件的kv
	var kvs []KeyValue // kv 的 slice，所有中间文件的kv

	for m := 0; m < nMap; m++ { // foreach map task
		tmp :=  reduceName(jobName, m, reduceTaskNumber) // 得到中间文件

		fp, err := os.Open(tmp) // 打开文件

		if err != nil {
			log.Fatal(err)
		}

		dec := json.NewDecoder(fp) // 创建decoder
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				log.Fatal(err)
			}
			kvs = append(kvs, kv) // 增加 kvs
		}

		fp.Close() // 关闭文件
	}


	// 2. 将kvs 按 k 排序，

	log.Println("kvs length: ", len(kvs))
	log.Println(kvs[0:5])

	sort.Sort(ByKey(kvs))
	log.Println(kvs[0:5])

	// 3. 同 k 聚集为 slice
	preKv := KeyValue{"", ""} // 前一行的kv

	var vs []string // 相同key的kvs

	fp, err := os.Create(outFile) // 创建reduce task的输出文件
	defer fp.Close()
	if err != nil {
		log.Fatal(err)
	}

	enc := json.NewEncoder(fp) //创建encoder

	for _, curKv := range kvs {

		if curKv.Key != preKv.Key && preKv.Key != "" { // 总结上一行结果 准备下一行
			enc.Encode(KeyValue{preKv.Key, reduceF(preKv.Key, vs)})

			vs = vs[:0] // 总结之后 vs置空
		}

		preKv = curKv
		vs = append(vs, curKv.Value)
	}
	if preKv.Key != "" {
		enc.Encode(KeyValue{preKv.Key, reduceF(preKv.Key, vs)})
	}
	log.Println(outFile)

	// 另外的方法， 直接用map[string][]string 即 map的key是keyValue的key，value是keyValue的value组成的数组


	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
}
