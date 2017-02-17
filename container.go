package mapreduce

import (
	"container/list"
	"fmt"
	"strconv"
	"time"
)

//MapReduce结构体/容器

type MRContainer struct {

	inBuffer *IOBuffer
	midBuffer *MidBuffer
	outBuffer *IOBuffer

	Mapper func(data interface{})MapperedDataEntry
	Reducer func(in MapperedDataSet) interface{}

	divider interface{}
	joiner interface{}

	MapperCount int //mapper线程数
	ReducerCount int //reduce线程数

	holdMapChans list.List // 当前掌握的Map工作协程管道，
	holdReduceChans list.List // 当前掌握的Reduce工作协程管道，

	isinit bool


}
//每个Map协程的管道集
type mapChanSet struct {
	key string
	in chan interface{}
	out chan MapperedDataEntry
	system chan int
	beep chan int
}
//每个reduce协程的管道集
type reduceChanSet struct {
	key string
	in chan MapperedDataSet
	out chan interface{}
	system chan int
	beep chan int
}

//***********配置方法************//

func (container *MRContainer) SetMapper (m func(data interface{})MapperedDataEntry) {
	container.Mapper = m
}
func (container *MRContainer) SetReducer (r func(in MapperedDataSet) interface{}) {
	container.Reducer = r
}
func (container *MRContainer) InsertData (v interface{}) {
	container.inBuffer.push(v)
}
func (container *MRContainer) GetResut () *list.List {
	return container.outBuffer.srcData
}
//******生命周期方法*******//

//初始化
func (container *MRContainer) Init () bool{
	fmt.Println("container has been initial")
	container.midBuffer = &MidBuffer{mapperdData:make(map[interface{}]list.List)}
	container.inBuffer = &IOBuffer{srcData:list.New()}
	container.outBuffer = &IOBuffer{srcData:list.New()}

	container.isinit = true
	return true
}

//启动容器
func (container *MRContainer) Start () error{

	i := 0
	//创建指定数目的map协程
	for ;i<container.MapperCount;i++ {
		cset := mapChanSet{in:make(chan interface{},1024),out:make(chan MapperedDataEntry,1024),system:make(chan int),beep:make(chan int),key:"map-"+strconv.Itoa(i)}
		container.holdMapChans.PushBack(cset)
		go container.mapWrapper(cset.in,cset.out,cset.system,cset.beep)
	}
	container.startMap()

	i = 0
	//创建指定数目的reduce协程
	for ;i<container.ReducerCount;i++ {
		cset := reduceChanSet{in:make(chan MapperedDataSet,1024),out:make(chan interface{},1024),system:make(chan int),beep:make(chan int),key:"reduce-"+strconv.Itoa(i)}
		container.holdReduceChans.PushBack(cset)
		go container.reduceWrapper(cset.in,cset.out,cset.system,cset.beep)
	}
	container.startReduce()
	return nil

}

//结束容器，当所有数据均处理完成后停止容器
func (container *MRContainer) Shutdown() {

}

//杀死容器，立刻停止容器工作
func (container *MRContainer) Kill (){

}

//*************工作方法*****************//

func (container *MRContainer) startMap (){
	fmt.Println("container doing map work")
	holder := container.holdMapChans.Front();
	inbuffer := container.inBuffer
	for  {
		fail2Stop := false // 判断是否可以停止map工作，当缓存清空且所有工作均已经完成即可

		if chans,ok := holder.Value.(mapChanSet);ok{
			select {
			case entry,ok :=<-chans.out:
				if !ok {
					fmt.Print("not ok !")
				}
				container.midBuffer.put(entry.key,entry.value)
				e := inbuffer.pull()
				if e!=nil {
					chans.in <- e
					fail2Stop = true //还有人没完成工作呢，哼哼
				}

			case state,ok := <-chans.beep:
				if !ok {
					fmt.Print("not ok !")
				}
				if state==0 {
					e := inbuffer.pull()
					if  e!=nil {
						chans.in <- e
						fail2Stop = true //还有人没完成工作呢，哼哼
					}else{
						chans.system <- SIGN_CTRL_SHUTDOWN // 当前已经没有更多数据，且此协程已经完成工作，发出关闭信号
					}
				}else{
					fail2Stop = true // 尝试停止服务失败，还有协程没有完成任务
				}
			}
		}
		if(!fail2Stop){
			break;
		}
		//维持轮询
		ele := holder.Next()
		if ele==nil{
			holder = container.holdMapChans.Front()
		}

	}

}

func (container *MRContainer) startReduce (){
	fmt.Println("container doing reduce work")
	maplist := list.New()
	//遍历解析map为list以做处理
	for k,v := range container.midBuffer.mapperdData{
		set := MapperedDataSet{key:k,data:v}
		maplist.PushBack(set)
	}
	holder := container.holdReduceChans.Front();
	for  {
		fail2Stop := false // 判断是否可以停止reduce工作，当缓存清空且所有工作均已经完成即可

		if chans,ok := holder.Value.(reduceChanSet);ok {
			select {
			case result,ok := <-chans.out:
				if !ok {
					fmt.Print("not ok !")
				}
				container.outBuffer.push(result)
				e := maplist.Front()
				if e!=nil {
					maplist.Remove(e)
				}
				if e != nil {
					if ele,ok:=e.Value.(MapperedDataSet);ok{
						chans.in <- ele
						fail2Stop = true //还有人没完成工作呢，哼哼
					}
				}

			case state,ok := <-chans.beep:
				if !ok {
					fmt.Print("not ok !")
				}
				if state == 0 {
					e := maplist.Front()
					if e!=nil {
						maplist.Remove(e)
					}
					if e != nil {
						if ele,ok:=e.Value.(MapperedDataSet);ok{
							chans.in <- ele
							fail2Stop = true //还有人没完成工作呢，哼哼
						}
					} else {
						chans.system <- SIGN_CTRL_SHUTDOWN // 当前已经没有更多数据，且此协程已经完成工作，发出关闭信号
					}
				} else {
					fail2Stop = true // 尝试停止服务失败，还有协程没有完成任务
				}
			}
		}
		if(!fail2Stop){
			break;
		}
		//维持轮询
		ele := holder.Next()
		if ele==nil{
			holder = container.holdReduceChans.Front()
		}

	}

}

//map协程方法
func (container *MRContainer) mapWrapper (in <-chan interface{}, out chan<- MapperedDataEntry, system chan int,cb chan int){
	mapper := container.Mapper
	workState := SIGN_BEEP_FREE // 当前工作状态，若为0，则可以输入，否则正忙
	beepChan := make(chan int)
	go beep(&workState,cb,beepChan) // 启动心跳
	for {
		shutdownFlag := false
		select {
		case src,ok := <-in:
			if !ok {
				fmt.Print("not ok !")
			}
			workState = SIGN_BEEP_WORKING
			entry := mapper(src)
			workState = SIGN_BEEP_FREE
			out<-entry

		case command,ok := <-system:
			if !ok {
				fmt.Print("not ok !")
			}
			fmt.Println("recive shutdown sign :", strconv.Itoa(command))
			if(command == SIGN_CTRL_SHUTDOWN){
				shutdownFlag = true
			}
		}

		if(shutdownFlag){
			break;
		}
	}
	fmt.Println("goroutines has been finish")
}

//reduce协程方法
func (container *MRContainer) reduceWrapper (in <-chan MapperedDataSet, out chan<-interface{}, system chan int, cb chan int){
	workState := SIGN_BEEP_FREE // 当前工作状态，若为0，则可以输入，否则正忙
	beepChan := make(chan int)
	go beep(&workState,cb,beepChan) // 启动心跳
	for {
		shutdownFlag := false
		select {
		case src,ok := <-in:
			if !ok {
				fmt.Print("not ok !")
			}
			workState = SIGN_BEEP_WORKING
			entry := container.Reducer(src)
			workState = SIGN_BEEP_FREE
			out<- entry

		case command,ok := <-system:
			if !ok {
				fmt.Print("not ok !")
			}
			if(command == SIGN_CTRL_SHUTDOWN){
				shutdownFlag = true
			}
		}

		if(shutdownFlag){
			break;
		}
	}
}

//心跳
func beep (state *int,cb chan<- int,ctrl <-chan int){
	for{
		kill := false
		select {
		case <-ctrl:
			kill = true;
		case cb <- *state:
			time.Sleep(5e9)
		}
		if kill{
			fmt.Println("kill an beep thread")
			break;
		}
	}
}

