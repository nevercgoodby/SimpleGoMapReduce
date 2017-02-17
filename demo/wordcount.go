package main

import (
	"mapreduce"
	"strconv"
	"fmt"
)

func main() {

	mrc := mapreduce.MRContainer{MapperCount:2,ReducerCount:2}
	mrc.Init()
	mrc.SetMapper(wdctMapper)
	mrc.SetReducer(wdctReduce)


	mrc.InsertData("hello")
	mrc.InsertData("hello")
	mrc.InsertData("world")
	mrc.InsertData("!")
	mrc.InsertData("Golang")
	mrc.InsertData("Go")
	mrc.InsertData("Golang")
	mrc.InsertData("aaa")
	mrc.InsertData("aaa")
	mrc.InsertData("Golang")
	mrc.InsertData("!")


	mrc.Start()


	result := mrc.GetResut()
	v  := result.Front()
	for {
		if v==nil {
			break;
		}
		fmt.Println(v.Value)
		v = v.Next()
	}
}



func  wdctMapper (data interface{}) mapreduce.MapperedDataEntry {

	mde := mapreduce.MapperedDataEntry{}
	mde.Set(data,1)
	return mde

}



func wdctReduce (data mapreduce.MapperedDataSet) interface{}{

	d := data.Data()
	len :=d.Len()
	key := data.Key()

	if k,ok := key.(string);ok {
		return k+":"+strconv.Itoa(len)
	}else{
		return "unknow key type : "+strconv.Itoa(len)
	}

}
