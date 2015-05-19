package triblab

import (
	"trib"
	"strings"
	"fmt"
	"time"
)

func NewBinClient(backs []string) trib.BinStorage {
	return &Binclient{backs:backs}
}

func ServeKeeper(kc *trib.KeeperConfig) error {

	//establish all connection with back-end
	allClient := make([]*client,len(kc.Backs))
	for i,addr := range kc.Backs{
		c := &client{addr:addr}
		allClient[i] = c
	}

	if kc.Ready != nil{
		go func(ch chan<- bool){
	       ch <- true
		}(kc.Ready)
	}

	for {
		<-time.Tick(1000*time.Millisecond)
		go move(allClient)
		go updateClock(allClient)
	}

	kc.Ready <- false
	return fmt.Errorf("keeper dies!")
}

func NewFront(s trib.BinStorage) trib.Server {
    return &nbserver{bc:s,allbin:make(map[string]trib.Storage)}
}

func CallKeeper(num int,keeperClient []*client) bool {
	for i:=0;i<num;i++{
		var ret uint64 
		e := keeperClient[i].Clock(0,&ret)
		if e == nil {
			return true
		}
	}
	return false
}

func updateClock(conncomb []*client) error{
	
	maxclock := uint64 (0)

	for j,_ := range conncomb{
		var ret uint64
		BinC := conncomb[j]
		e := BinC.Clock(0, &ret)
		if e != nil{
			continue
		}
		if ret > maxclock{
			maxclock = ret
		}
	}

	for x,_ := range conncomb{
		var ret uint64
		BinC := conncomb[x]
		e := BinC.Clock(maxclock, &ret)
		if e != nil{
			continue
		}
	}
	return nil
}

//get all bin names from all backs
func GetAllBin(allClient []*client) map[string][]int{
	ret := make(map[string][]int)

	//scan all the clients
	for i,c := range allClient{
		//get the keys through the ::flag
		p := trib.Pattern{Prefix:"",Suffix:"::flag"}
		var binList trib.List

		//add the keys to the ret slices
		_ = c.Keys(&p,&binList)
		for j := range binList.L{
			trimedKey := strings.TrimSuffix(binList.L[j],"::flag")
			//add the client's index to the result's int array
			_,found := ret[trimedKey]
			if found{
				ret[trimedKey] = append(ret[trimedKey],i)
			} else{
				ret[trimedKey] = []int{i}
			}
		}
	}
	for key,num := range ret{
		if len(num) >= 3 {
			delete(ret,key)
		}
	}
	return ret
}

//the function to merge two lists
func Merge(a, b trib.List) trib.List{
	m := make(map[string]bool)
	for _, item := range(a.L){
		m[item] = true
	}
	for _, item := range(b.L){
		m[item] = true
	}
	var list trib.List
	list.L = []string{}
    for i, _ := range(m){
	list.L = append(list.L, i)
    }
    return list
}

func BackUp(binName string,backupClient []*client) map[string]trib.List{
	//unique binName
	binName = binName+"::"
	p := trib.Pattern{Prefix:binName,Suffix:""}
	var keys1 trib.List
	var keys2 trib.List

	//fmt.Println(len(backupClient))
	m := make(map[string]trib.List)

	if (len(backupClient)<2){
	  	return m//return BackUp_1(binName,backupClient)
	}
	p1 := p
	_ = backupClient[0].ListKeys(&p1,&keys1)
	p2 := p
	_ = backupClient[1].ListKeys(&p2,&keys2)

	for _,s := range keys1.L{
		m[s]=BackUphelper(s,backupClient)
	}

	for _,s := range keys2.L{
		_,found := m[s]
		if !found {
			m[s] = BackUphelper(s,backupClient)
		}
	}
	return m
}
/*
func BackUp1(binName string,backupClient []*client) map[string]trib.List{
	binName = binName+"::"
	var keys trib.List
	m := make(map[string]trib.List)
	p := trib.Pattern{Prefix:binName,Suffic:""}
	_ = backup[0].ListKeys(&p,&keys)

	for _,s := range keys.

*/

//get the maximal List of one specific key
func BackUphelper(key string,backupClient []*client) trib.List{
	var list1 trib.List
	var list2 trib.List

	backupClient[0].ListGet(key,&list1)
	backupClient[1].ListGet(key,&list2)
	if len(list1.L)==0{
		return list2
	}
	if len(list2.L)==0{
		return list1
	}
	return Merge(list1,list2)
}

func move(allClient []*client) {
	//map from bin to back
	BinBack := GetAllBin(allClient)
	//count := 0
	for _,intcomb:= range BinBack{
	  	fmt.Println(intcomb)
	}
	for Binname,indexcomb := range BinBack{
		backupClient := make([]*client,0)
		fmt.Println(Binname)
		for _,i := range indexcomb{
			backupClient = append(backupClient,allClient[i])
		}
//	fmt.Println(len(backupClient))
		moveMap := BackUp(Binname,backupClient)

		dest := allClient[FindNext(allClient,indexcomb)]

		for key,list := range moveMap{
			var destlist trib.List
			_ = dest.ListGet(key,&destlist)

			//get the final list
			destlist = Diff(list,destlist)

			//append the value in the list to the destination
			for _,value := range destlist.L{
				succ := false
				_ = dest.ListAppend(&trib.KeyValue{Key:key,Value:value},&succ)
			}
		}

		succ := false
		//set the rep flag
		_ = dest.Set(&trib.KeyValue{Key:Binname + "::flag", Value:"on"},&succ)
	}
}

func Diff(source,dest trib.List) trib.List{
  	diff := make(map[string]bool)
	for _,s := range source.L{
	 	diff[s] = true
	}
	for _,s := range dest.L{
		_,found := diff[s]
		if found{
			delete(diff,s)
		}
	}

	var ret trib.List
	for str,_ := range diff{
		ret.L = append(ret.L,str)
	}
	return ret
}

func FindNext(allClient []*client,backupIndex []int) int{
	max := 0
	for _,index := range backupIndex{
		if index > max{
			max = index
		}
	}


	for i := 1;i<len(allClient);i++{
		currIndex := i + max
		if Repeat(currIndex%len(allClient),backupIndex){
			continue
		}
		var ret uint64 
		e := allClient[currIndex%len(allClient)].Clock(0,&ret)
		if e == nil{
			fmt.Println(currIndex%len(allClient))
			return currIndex%len(allClient)
		}
	}
	return max
}

func Repeat(currIndex int,backupIndex []int) bool{
	for _,i := range backupIndex{
		if currIndex == i{
			return true
		}
	}
	return false
}
