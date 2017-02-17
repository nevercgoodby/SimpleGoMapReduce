package main

import (
	"fmt"
	"container/list"
	"strconv"
)

func main() {
	fmt.Println("Map Reduce Demo started!")

	buffer := &IOBuffer{srcData:list.New()}
	buffer.push(1)
	buffer.push(1)
	buffer.push(1)
	buffer.push(1)
	buffer.push(1)

	fmt.Println(strconv.Itoa(buffer.len()))
	buffer.pull()
	fmt.Println(strconv.Itoa(buffer.len()))

}

//输入输出数据缓存区
type IOBuffer struct {
	srcData *list.List
}

func (buff *IOBuffer) push(e interface{}){
	buff.srcData.PushBack(e)
}
func (buff *IOBuffer) pull () interface{} {
	sd := buff.srcData
	e := sd.Front()
	if e != nil {
		sd.Remove(sd.Front())

	}

	return e.Value
}
func (buff *IOBuffer) len() int {
	return buff.srcData.Len()
}