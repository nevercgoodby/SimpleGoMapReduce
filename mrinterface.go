package mapreduce

import "container/list"

//mapper接口，用于映射数据
type Mapper interface {
	mapper (data interface{}) MapperedDataEntry
}

//Reduce接口，用于映射数据
type Reducer interface {
	reduce (in MapperedDataSet) interface{}
}

type MapperedDataEntry struct {
	key,value interface{}
}
func (e *MapperedDataEntry) Set(k,v interface{}){
	e.key,e.value = k,v
}

type MapperedDataSet struct {
	key interface{}
	data list.List
}
func (e *MapperedDataSet) Set(k interface{},v list.List){
	e.key,e.data = k,v
}
func (e *MapperedDataSet) Key() interface{}{
	return e.key
}
func (e *MapperedDataSet) Data() list.List{
	return e.data
}
