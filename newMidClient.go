package triblab

import (
    . "trib"
	"encoding/json"
	"sort"
	"fmt"
	"strings"
)

type logs []*logg

type newMidClient struct{
	backs []string
	clientcomb []*midClient
	indexcomb []int
	name string
	conncomb []*midClient
}


type logg struct{
	Value string
	Clock uint64
}


func (self *newMidClient) Get(key string, value *string) error{
	self.Check()
	//oldvalue := make([]List,3)  //get the oldvalue of kv.Value in order to remove in the next step
	var e error
	numOfremove := 0;
	var allthelogs logs
	allthelogs = []*logg{}

	thekey := "0::" + key
	for i := 0; i < len(self.clientcomb); i++{
		var list List

		e = self.clientcomb[i].ListGet(thekey, &list)
		if e != nil{
			return e
		}
		for j := range(list.L){
			var templog logg
			e = json.Unmarshal([]byte (list.L[j]), &templog)
			if e != nil{
				return e
			}
			allthelogs = append(allthelogs, &templog)
		}
	}

	sort.Sort(allthelogs)
	if len(allthelogs)==0 {
		*value = ""
		return nil
	}

	*value = (allthelogs[len(allthelogs)-1]).Value

	/*
	for i := 0; i < len(allthelogs); i++{

	  	rmlog,rme := json.Marshal(*allthelogs[i])
		if rme != nil{
			return rme
		}
		for j := 0; j < len(self.clientcomb); j++{
			kv := KeyValue{Key: thekey, Value: string(rmlog)}
			e = self.clientcomb[j].ListRemove(&kv, &numOfremove)
			if e != nil{
				return e
			}
		}
	}

	for i := 0; i < len(self.clientcomb); i++ {
		aplog,ape := json.Marshal(*allthelogs[len(allthelogs)-1])
		if ape != nil{
			return ape
		}
		succ := false
		ape = self.clientcomb[i].ListAppend(KeyValue{Key:thekey,Value:apLog},&succ)
		if ape != nil || succ == false{
			return ape																																		
		}											
	}*/

	for i := 0; i < len(allthelogs)-1; i++{
	    if *allthelogs[i] == *allthelogs[len(allthelogs)-1]{
			continue
		}
		rmlog,rme := json.Marshal(*allthelogs[i])
		if rme != nil{
			return e
		}
		for j := 0; j < len(self.clientcomb); j++{
			kv := KeyValue{Key: thekey, Value: string(rmlog)}
			e = self.clientcomb[j].ListRemove(&kv, &numOfremove)
			if e != nil{
				return e
			}
		}
	}
	return nil
}

func (self *newMidClient) Set(kv *KeyValue, succ *bool) error{
	self.Check()
	var ret uint64

	//get the max clock
	_ = self.Clock(0,&ret)

	for i := 0; i < len(self.clientcomb); i++{
		thekey := "0::" + kv.Key
		thevalue := kv.Value
		thelog, e := json.Marshal(logg{Value:thevalue, Clock:ret})
		if e != nil{
			return e
		}
		e = self.clientcomb[i].ListAppend(&KeyValue{thekey,string(thelog)}, succ)
		fmt.Println("set")
		if e != nil || *succ == false{
			return e
		}
	}
	return nil
}

func (self *newMidClient) Keys(p *Pattern, list *List) error{
    self.Check()
    thekey := *p
    m := make(map[string]bool)
    for i := 0; i < len(self.clientcomb); i++{
    	var nblist List
    	*p = thekey
    	p.Prefix = "0::" + p.Prefix
		e := self.clientcomb[i].ListKeys(p, &nblist)
		if e != nil{
			return e
		}
		for _, j := range(nblist.L){
			m[j] = true
		}

    }
    list.L = []string{}
    for i, _ := range(m){
    	list.L = append(list.L, strings.TrimPrefix(i, "0::"))
    }

    return nil
 }





func (self *newMidClient) ListGet(key string, list *List) error{
	//check and get 3 available midClient
	self.Check()

	var e error

	//the log slice to store all the append and remove logs
	rmLogComb := make([]logs, len(self.clientcomb))
	apLogComb := make([]logs, len(self.clientcomb))

	removeKey := "10::" + key
	appendKey := "11::" + key

	for i := 0; i < len(self.clientcomb); i++{	

		//to store the got list
		var rmList List
		var apList List

		//to get all remove logs
		e = self.clientcomb[i].ListGet(removeKey,&rmList)
		if e != nil{
			return e
		}

		//move the logs on list to rmLogComb
		for j := 0; j < len(rmList.L); j++{
			var rmMidLog logg
			e = json.Unmarshal([]byte (rmList.L[j]), &rmMidLog)
			if e != nil{
				return e
			}

			//all the remove logs should be removed
			var ret int
			e = self.clientcomb[i].ListRemove(&KeyValue{removeKey,rmList.L[j]},&ret)
			if e != nil{
				return e
			}

			//append the log to the log comb
			rmLogComb[i] = append(rmLogComb[i],&rmMidLog)
		}
		sort.Sort(rmLogComb[i])

		//to get all append logs
		e = self.clientcomb[i].ListGet(appendKey,&apList)
		if e != nil{
			return e
		}

		//move the logs on list to rmLogComb
		for j := 0; j < len(apList.L); j++{
			var apMidLog logg
			e = json.Unmarshal([]byte (apList.L[j]), &apMidLog)
			if e != nil{
				return e
			}

			apLogComb[i] = append(apLogComb[i],&apMidLog)
		}
		sort.Sort(apLogComb[i])
	}

	//the total map containing all the data
	totalCheck := make(map[logg]bool)

	for i := 0; i < len(self.clientcomb); i++{
		//the list for the value not been removed
		apCheck := []logg{}

		//check all append log
		for _,apInfo := range apLogComb[i]{
			var j int
			//check remove log
			for j = len(rmLogComb[i]) - 1; j >= 0; j--{
				//if value equals
				if apInfo.Value == rmLogComb[i][j].Value{
					if apInfo.Clock > rmLogComb[i][j].Clock{
						apCheck = append(apCheck,*apInfo)
					} else{
						newval,_ := json.Marshal(apInfo)
						//otherwise remove it from the backend
						var ret int
						_ = self.clientcomb[i].ListRemove(&KeyValue{appendKey,string(newval)},&ret)
						break
					}
				}	
			}
			if j < 0{
				apCheck = append(apCheck,*apInfo)
			}
		}

		for _,apLog := range apCheck{
			totalCheck[apLog] = true
		}
	}

	//append all the data to the list.L
 	list.L = []string{}
	for val,_ := range totalCheck{
		list.L = append(list.L,val.Value)
	}

	return nil
}




func (self *newMidClient) ListAppend(kv *KeyValue, succ *bool) error{
	//"11" means list append
	apKey := "11::" + kv.Key 
	tempValue := kv.Value

	//check and get 3 available midClient
	self.Check()

	var clo uint64

	//get the max clock
	_ = self.Clock(0,&clo)

    *succ = false
	for i := 0; i < len(self.clientcomb); i++{
  		var nlog logg
  		var e error
		nlog.Value = tempValue
		nlog.Clock = clo
		compactM,compactE := json.Marshal(nlog)
		if compactE != nil{
			return compactE
		}

		e = self.clientcomb[i].ListAppend(&KeyValue{Key:apKey,Value:string(compactM)},succ)
		if e != nil || *succ != true{
			return fmt.Errorf("Fail to append the value")
		}
	}

	return nil
}





func (self *newMidClient) ListRemove(kv *KeyValue, n *int) error{
	apKey := "11::" + kv.Key

	//"10" means list remove
	kv.Key = "10::" + kv.Key
	tempValue := kv.Value

	//check and get 3 available midClient
	self.Check()

	var clo uint64

	//get the max clock
	_ = self.Clock(0,&clo)

	max := 0

	for i := 0; i < len(self.clientcomb); i++{
  		var nlog logg
  		var e error
		nlog.Value = tempValue
		nlog.Clock = clo
		compactM,compactE := json.Marshal(nlog)
		if compactE != nil{
			return compactE
		}
		kv.Value = string(compactM)
		var succ bool
		e = self.clientcomb[i].ListAppend(kv,&succ)
		if e != nil || succ != true{
			return fmt.Errorf("Fail to remove the value")
		}

		var apList List
		_ = self.clientcomb[i].ListGet(apKey,&apList)

		var apLogComb logs
		//move the logs on list to rmLogComb
		for j := 0; j < len(apList.L); j++{
			var apMidLog logg
			e = json.Unmarshal([]byte (apList.L[j]), &apMidLog)
			if e != nil{
				return e
			}
			apLogComb = append(apLogComb,&apMidLog)
		}

		count := 0
		//check the append log with the same value, remove them
		for _,apInfo := range apLogComb{
			if apInfo.Value == tempValue{
				count++
				newval,_ := json.Marshal(apInfo)
				//otherwise remove it from the backend
				var ret int
				_ = self.clientcomb[i].ListRemove(&KeyValue{apKey,string(newval)},&ret)
			}
		}
		if count > max {
			max = count
		}
	}
	*n = max
	return nil
}

func (self *newMidClient) ListKeys(p *Pattern, list *List) error{
    
    var e error

    //the list to get the remove and appned list
    self.Check()
    var apList List
    var rmList List

    totalList := make(map[string]bool)

    //get them
    for i := range self.clientcomb{
    	//the two patterns to get the keys from append and remove
    	apPattern := *p
    	rmPattern := *p

    	//different prefix
    	apPattern.Prefix = "11::" + apPattern.Prefix
    	rmPattern.Prefix = "10::" + rmPattern.Prefix

    	e = self.clientcomb[i].ListKeys(&apPattern,&apList)
		if e != nil{
		return e
		}
		for k := range apList.L{
	  		apList.L[k] = strings.TrimPrefix(apList.L[k], "11::")
		}

		e = self.clientcomb[i].ListKeys(&rmPattern,&rmList)
		if e != nil{
		return e
		}
		for k := range rmList.L{
	  		rmList.L[k] = strings.TrimPrefix(rmList.L[k], "10::")
		}

		//check the relation between the append list and the remove list
		for _,apKey := range apList.L{
			rep := false
			for j := range rmList.L{
				if rmList.L[j] == apKey{
					rep = true
					break
				}
			}
			//if it is not removed
			if !rep{
				totalList[apKey] = true
			} else {
				//if it is removed, check the clock
				isRm := self.IsRemove(apKey,i)
				if !isRm {
					totalList[apKey] = true
				}
			}
		}
    }
	list.L = []string{}

	//move the total result to the returned list
	for apKey,_ := range totalList{
		list.L = append(list.L,apKey)
	}

	return nil
}

func (self *newMidClient) Clock(atLeast uint64, ret *uint64) error{
	self.Check()
	max := uint64(0)
	for i := 0; i < len(self.clientcomb); i++{
		e := self.clientcomb[i].Clock(atLeast,ret)
		if e != nil{
			return e
		}
		if *ret > max{
			max = *ret
		}
	}
	*ret = max
	return nil
}



func (self *newMidClient) isConnect(c *midClient) bool{
	var value string
	key := "flag"
	e := c.Get(key,&value)
	if e==nil && value =="on"{
		return true
	}
	return false
}


func (self *newMidClient) Check(){
	//return
	max := 0
	for _,i := range self.indexcomb{
		if i > max{
			max = i
		}
	}

	//index
	newclientcomb := make([]*midClient,0)
	newindexcomb := make([]int,0)
	count := 0
	//return
	for i,localclient := range self.clientcomb{
		if self.isConnect(localclient) {
			newclientcomb = append(newclientcomb,localclient)
			newindexcomb = append(newindexcomb,self.indexcomb[i])
			count++
		}
	}

	if count >=3{
		return
	}
	for i:=1;i<len(self.backs);i++ {
		curr_index := (max + i)%len(self.backs)
		c := self.conncomb[curr_index]

		if self.CheckRepeat(curr_index,self.indexcomb){
			continue
		}
		if self.isConnect(c) {
			newclientcomb = append(newclientcomb,c)
			newindexcomb = append(newindexcomb,curr_index)
			count++
		}
		if count>=3 {
			break
		}
	}
	/*for _,cl := range self.clientcomb{
		cl.Selclient.conn.Close()
	}*/
	self.clientcomb = newclientcomb
	self.indexcomb = newindexcomb
}



func (self *newMidClient) CheckRepeat(i int, array []int) bool {
	for _,j := range array{
		if i==j {
			return true
		}
	}
	return false
}




func (s logs) Len() int{
	return len(s)
}
func (s logs) Swap(i, j int){
	s[i], s[j] = s[j], s[i]
}
func (s logs) Less(i, j int) bool{
	return s[i].Clock < s[j].Clock
}

func (self *newMidClient) IsRemove(key string,i int) bool{
	apKey := "11::"+key
	rmKey := "10::"+key
	var apList List
	var rmList List
	var e error
	//get two list
	self.clientcomb[i].ListGet(apKey,&apList)
	self.clientcomb[i].ListGet(rmKey,&rmList)
	//array of log
	apLogComb := make([]logs, len(self.clientcomb))
	rmLogComb := make([]logs, len(self.clientcomb))
	//add log to array
	for j := 0; j < len(apList.L); j++{
		var apMidLog logg
		e = json.Unmarshal([]byte (apList.L[j]), &apMidLog)
		if e != nil{
			return false
		}

		apLogComb[i] = append(apLogComb[i],&apMidLog)
	}
	sort.Sort(apLogComb[i])
	for j := 0; j < len(rmList.L); j++{
		var apMidLog logg
		e = json.Unmarshal([]byte (rmList.L[j]), &apMidLog)
		if e != nil{
			return false
		}

		rmLogComb[i] = append(rmLogComb[i],&apMidLog)
	}
	//map 
	apCheck := make(map[string]uint64)

	//scan all the append log from new to old
	for j := len(apLogComb[i])-1; j >= 0; j--{
		//if not found this value, insert it
		_,found := apCheck[apLogComb[i][j].Value]
		if found == false {
			apCheck[apLogComb[i][j].Value] = apLogComb[i][j].Clock
		}
	}

	for j := 0; j < len(rmLogComb[i]); j++{
		apClock,found := apCheck[rmLogComb[i][j].Value]
		//if the appended has been removed
		if found == true && apClock < rmLogComb[i][j].Clock{
			delete (apCheck,rmLogComb[i][j].Value)
		}
	}

	count := 0
	for _,_ = range apCheck{
		count++
	}
	if count==0 {
		return true
	}
	return false
}
