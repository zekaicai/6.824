package main

import (
	"encoding/json"
	"fmt"
	"os"
)


type KeyValue struct {
	Key   string
	Value string
}
func main() {

	createJson()
}

func readJson() {

}

func createJson() {
	file, _ := os.Create("test.txt")
	enc := json.NewEncoder(file)
	var in [3]KeyValue
	in[0] = KeyValue{"1", "a"}
	in[1] = KeyValue{"2", "b"}
	in[2] = KeyValue{"3", "c"}
	for _, kv := range in {
		err := enc.Encode(&kv)
		if err != nil {

			fmt.Print("error")
		}
	}
}
