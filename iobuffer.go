package mapreduce

import (
	"container/list"
)



//中间数据缓存区
type MidBuffer struct {
	mapperdData map[interface{}]list.List
}

func (buff *MidBuffer) put(key,data interface{}){
	datalist := buff.mapperdData[key]
	datalist.PushBack(data)
	buff.mapperdData[key] = datalist
}

func (buff *MidBuffer) get(key interface{}) list.List{
	return buff.mapperdData[key]
}

//输入输出数据缓存区
type IOBuffer struct {
	srcData *list.List
}

func (buff *IOBuffer) push(e interface{}){
	l := buff.srcData
	l.PushBack(e)
}
func (buff *IOBuffer) pull () interface{} {
	sd := buff.srcData
	e := sd.Front()

	if e != nil {
		sd.Remove(sd.Front())
	}else{
		return nil
	}

	return e.Value
}
func (buff *IOBuffer) len() int {
	return buff.srcData.Len()
}