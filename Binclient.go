package triblab

import(
	"hash/fnv"
	. "trib"
//	"fmt"
)

type Binclient struct{
	backs []string
}

func (self *Binclient)Bin(name string) Storage{
	h := fnv.New32()
	h.Write([]byte (name))
	s := int (h.Sum32())%len(self.backs)

	conncomb := make([]*midClient,0)
	clientcomb := make([]*midClient,0)
	indexcomb := make([]int,0)
	count := 0

	/*for i := 0; i < len(self.backs); i++{
		curr_index := s+i
		c := &midClient{Selclient:&client{addr:self.backs[curr_index%len(self.backs)],conn:nil},Binname:name}
		conncomb = append(conncomb,c)
		var ret uint64
		e := c.Clock(0, &ret)
		if e == nil {
			if count < 1{
				var succ bool
				kv := &KeyValue{"flag","on"}
				e := c.Set(kv,&succ)
				clientcomb = append(clientcomb,c)
				indexcomb = append(indexcomb,curr_index%len(self.backs))
				if e!=nil || succ==false{
					continue
				}
			}
					//fmt.Println(curr_index)
			count++
		}
	}*/
		/*if count >= 1{
			break
		}*/

	for i := 0; i < len(self.backs); i++{
		curr_index :=(s+i)%len(self.backs)
		c := &midClient{Selclient:&client{addr:self.backs[curr_index],conn:nil},Binname:name}
		conncomb = append(conncomb,c)
		if count < 3{
			kv := &KeyValue{"flag","on"}
			var succ bool
			e := c.Set(kv,&succ)
			if e==nil && succ == true{
				count++
				clientcomb = append(clientcomb,c)
				indexcomb = append(indexcomb,curr_index)
			}
		}
	}

	return &newMidClient{backs:self.backs,clientcomb:clientcomb,indexcomb:indexcomb,name:name,conncomb:conncomb}
}



