package triblab

import(
	"fmt"
	. "trib"
	"sort"
	"encoding/json"
	"time"
	"math/rand"
	"strings"
	"strconv"
)

type conTrib []*Trib

type nbserver struct{
	bc BinStorage
	//used to store all previous bin
	allbin map[string]Storage
}

func (self *nbserver) SignUp(user string) error{
	if !IsValidUsername(user) {
		return fmt.Errorf("username %q wrong",user)
	}

	//conver the username to lower case
	user = strings.ToLower(user)

	//if the user name equals to "userList"
	if user == "userList"{
		return fmt.Errorf("user %q conflicts with the existing name", user)
	}

	//get a midclient
	c := self.getBin(user)

	//check if the user exists
	b, e := self.checkExist(c, user)
	if e != nil{
		return e
	}
	if b == true{
		return fmt.Errorf("user %q already exists", user)
	}

	//if not, set it to "signed"
	setk := KeyValue{Key:user, Value:"signed"}
	var succ bool
	succ = false
	e = c.Set(&setk,&succ)

	if e != nil || succ == false{
		return fmt.Errorf("failed to SignUp user %q", user)
	}

	//add the user to the list to all back ends if less than 20
	for i := 0; i < 20; i++ {
		var l List
		c := self.getBin(strconv.Itoa(i))
		e = c.ListGet("userList",&l)
		if e != nil{
			return e
		}
		l.L = self.delMultiple(l.L)
		if len(l.L) >= 20 {
			continue
		}

		succ = false
		newUser := KeyValue{Key:"userList", Value:user}
		e = c.ListAppend(&newUser, &succ)

		if e != nil || succ == false{
			return fmt.Errorf("failed to append the user %q", user)
		}
	}
	return nil
}





func (self *nbserver) ListUsers() ([]string, error){
	//get a random number to locate one backend
	rand.Seed(time.Now().UnixNano())
	x := rand.Intn(20)

	ret := make([]string, 0, 20)

	//get the userlist
	var l List
	c := self.getBin(strconv.Itoa(x))
	e := c.ListGet("userList", &l)
	if e != nil{
		return nil, e
	}

	//renew all the lists
	for i := range l.L {
		ret = append(ret, l.L[i])
	}

	ret = self.delMultiple(ret)
	sort.Strings(ret)

	return ret, nil
}




func (self *nbserver) Post(who, post string, clock uint64) error{
	if len(post) > MaxTribLen {
		return fmt.Errorf("trib too long")
	}
	
	who = strings.ToLower(who)

	c := self.getBin(who)
	
	//check user
	b, e := self.checkExist(c, who)
	if e != nil{
		return e
	}
	if b == false{
		return fmt.Errorf("user %q not exists", who)
	}

	//renew the clock in backend if needed
	var currC uint64
	e = c.Clock(clock, &currC)
	if e != nil{
		return e
	}

	compactM, compacte := json.Marshal(Trib{User:who, Message:post, Time:time.Now(), Clock: currC})
	if compacte != nil{
		return compacte
	}

	//post the trib on the user's timeline
	var succ bool
	succ = false 
	selfTri := KV("Trib-", string(compactM))
	e = c.ListAppend(selfTri, &succ)
	if e != nil || succ != true {
		return fmt.Errorf("failed to post %q", who)
	}

	return nil
}

func (self *nbserver) Tribs(user string) ([]*Trib, error){

	var ret conTrib
	user = strings.ToLower(user)

	c := self.getBin(user)

	//check if the user exists
	b, e := self.checkExist(c, user)
	if e != nil{
		return nil,e
	}
	if b == false{
		return nil,fmt.Errorf("user %q not exists", user)
	}

	//get the list from the user
	var l List
	e = c.ListGet("Trib-", &l)

	if e != nil{
		return nil, e
	}

	for i := range l.L{
		var medtrib Trib
		e = json.Unmarshal([]byte (l.L[i]), &medtrib)
		if e != nil{
			return nil, e
		}
		ret = append(ret, &medtrib)
	}

	sort.Sort(ret)

	//do garbage collection
	if len(ret) >= 200{
		for i := 0; i <= len(ret) - 200; i++{
			compactM, _ := json.Marshal(ret[i])
			rmkv := KeyValue{Key:"Trib-",Value:string (compactM)}
			n := 0
			e = c.ListRemove(&rmkv,&n)
		}
	}

	//get the newest 100 tribs
	if len(ret) > MaxTribFetch{
		ret = ret[len(ret) - MaxTribFetch:]
	}

	return ret, nil
}

func (self *nbserver) Follow(who, whom string) error{
	who = strings.ToLower(who)
	whom = strings.ToLower(whom)
	if who == whom {
		return fmt.Errorf("cannot follow oneself")
	}

	c := self.getBin(who)

	//check user
	b, e := self.checkExist(c, who)
	if e != nil{
		return e
	}
	if b == false{
		return fmt.Errorf("user %q not exists", who)
	}

	//check whom
	mc := self.getBin(whom)
	mb, me := self.checkExist(mc, whom)
	if me != nil{
		return me
	}
	if mb == false{
		return fmt.Errorf("user %q not exists", whom)
	}

	//check if who has followed whom
	succ, e := self.IsFollowing(who,whom)

	if e != nil {
		return e
	}

	if succ == true {
		return fmt.Errorf("user %q already following %q", who, whom)
	}

	//get the list from who
	var l List
	e = c.ListGet("Following-", &l)
	if e != nil {
		return e
	}
	l.L = self.delMultiple(l.L)
	if len(l.L) >= MaxFollowing{
		return fmt.Errorf("user %q is following too many users", who)
	}

	//then add one whom to the following list
	var win bool
	win = false
	followingKV := KeyValue{"Following-", whom}
	e = c.ListAppend(&followingKV, &win)

	if e != nil || win != true {
		return e
	}

	return nil
}

func (self *nbserver) Unfollow(who, whom string) error{
	who = strings.ToLower(who)
	whom = strings.ToLower(whom)
	if who == whom {
		return fmt.Errorf("cannot unfollow oneself")
	}

	//check who
	c := self.getBin(who)

	//check user
	b, e := self.checkExist(c, who)
	if e != nil{
		return e
	}
	if b == false{
		return fmt.Errorf("user %q not exists", who)
	}

	//check whom
	mc := self.getBin(whom)

	mb, me := self.checkExist(mc, whom)
	if me != nil{
		return e
	}
	if mb == false{
		return fmt.Errorf("user %q not exists", whom)
	}

	//check if who is following whom
	succ, fe := self.IsFollowing(who,whom)

	if fe != nil {
		return e
	}

	if succ == false {
		return fmt.Errorf("user %q not following %q", who, whom)
	}

	//delete the whom

	n := 1
	followingKV := KV("Following-", whom)
	e = c.ListRemove(followingKV,&n)
	if e != nil {
		return e
	}

	return nil
}

func (self *nbserver) IsFollowing(who, whom string) (bool,error){
	who = strings.ToLower(who)
	whom = strings.ToLower(whom)
	if who == whom {
		return false, fmt.Errorf("checking the same user")
	}

	c := self.getBin(who)

	//check who
	b, e := self.checkExist(c, who)
	if e != nil{
		return false, e
	}
	if b == false{
		return false,fmt.Errorf("user %q not exists", who)
	}

	//check whom
	mc := self.getBin(whom)
	mb, me := self.checkExist(mc, whom)
	if me != nil{
		return false, e
	}
	if mb == false{
		return false,fmt.Errorf("user %q not exists", whom)
	}

	var l List

	e = c.ListGet("Following-", &l)
	if e != nil{
		return false, e
	}

	for i := range l.L{
		if l.L[i] == whom {
			return true, nil
		}
	}

	return false, nil
}

func (self *nbserver) Following(who string) ([]string,error){
	who = strings.ToLower(who)
	
	c := self.getBin(who)

	//check user
	b, e := self.checkExist(c, who)
	if e != nil{
		return nil,e
	}
	if b == false{
		return nil,fmt.Errorf("user %q not exists", who)
	}

	var l List
	ret := []string{}
	e = c.ListGet("Following-", &l)
	if e != nil{
		return nil, e
	}

	m := make(map[string]bool)
	//get the following users
	for _,s := range l.L{
		if m[s] !=true{
			m[s] = true
		}
	}
	for s,_ := range m{
		ret = append(ret,s)
		if len(ret) >= MaxTribFetch{
			return ret,nil
		}
	}
	return ret,nil
}

func (self *nbserver) Home(user string) ([]*Trib,error){
	user = strings.ToLower(user)

	c := self.getBin(user)

	//check user
	b, e := self.checkExist(c, user)
	if e != nil{
		return nil,e
	}
	if b == false{
		return nil,fmt.Errorf("user %q not exists", user)
	}

	//get the list of this user
	var l List
	e = c.ListGet("Trib-", &l)
	if e != nil{
		return nil, e
	}

	var ret conTrib
	var selfe error
	ret = []*Trib{}
	
	ret, selfe = self.Tribs(user)
	if selfe != nil{
		return nil,selfe
	}
	if ret == nil{
		ret = []*Trib{}
	}

	//get the tribs from other users

	e = c.ListGet("Following-", &l)
	if e != nil{
		return nil, e
	}

	totret := make(chan []*Trib)
	//iterate all the following users
	for x := range l.L{
		go func(conaddr string){
			newl,_ := self.Tribs(conaddr)
			totret <- newl
			
		}(l.L[x])
	}

	soret := make([][]*Trib, len(l.L))
	for in := 0; in < len(l.L); in++ {
		temp := <- totret
		soret[in] = temp
	}

	for d := 0; d < len(l.L); d++{
		if soret[d] != nil{
			for index,_ := range soret[d]{
				ret = append(ret,soret[d][index])
			}
		} 
	}

	sort.Sort(ret)
	if len(ret) > MaxTribFetch{
		ret = ret[len(ret) - MaxTribFetch:]
	}

	return ret, nil
}

func (self *nbserver) checkExist(c Storage, user string) (bool,error){
	var check string

	//check if user exists
	e := c.Get(user,&check)
	if e != nil{
		return false, e
	}

	if check == "signed"{
		return true, nil
	}
	return false, nil
}

//used to get and save the successful bins
func (self *nbserver) getBin(user string) Storage{
	bin := self.allbin[user]
	if bin == nil{
		bin = self.bc.Bin(user)
		self.allbin[user] = bin
	}
	return bin
}

//three functions used for sort []*Trib
func (self conTrib) Len() int{
	return len(self)
}

func (self conTrib) Less(i, j int) bool{
	if self[i].Clock != self[j].Clock {
		return self[i].Clock < self[j].Clock
	} else if self[i].Time != self[j].Time {
		return self[i].Time.UnixNano() < self[j].Time.UnixNano()
	} else if self[i].User != self[j].User {
		return self[i].User < self[j].User
	} else {
		return self[i].Message < self[j].Message
	}
}

func (self conTrib) Swap(i, j int) {
	temp := self[i]
	self[i] = self[j]
	self[j] = temp
}




func (self *nbserver) delMultiple(array []string) []string{

	m := make(map[string]bool)
	ret := make([]string,0)

	for _,s := range array{
		if m[s] == true{
			continue
		}
		m[s] = true
	}

	for s,_ := range m{
		ret = append(ret,s)
	}

	return ret
}



