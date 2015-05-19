package triblab

import (
    . "trib"
	"strings"
)

type midClient struct{
	Selclient *client
    Binname string
}

func (self *midClient) Get(key string, value *string) error{
	return self.Selclient.Get(self.Binname + "::" + key, value)
}

func (self *midClient) Set(kv *KeyValue, succ *bool) error{
	kv.Key = self.Binname + "::" + kv.Key
	return self.Selclient.Set(kv,succ)
}

func (self *midClient) Keys(p *Pattern, list *List) error{
    p.Prefix = self.Binname + "::" + p.Prefix
	e := self.Selclient.Keys(p,list)
	if e != nil{
		return e
	}
	for k := range list.L{
	  list.L[k] = strings.TrimPrefix(list.L[k], self.Binname + "::")
	}
	return nil
}

func (self *midClient) ListGet(key string, list *List) error{
	return self.Selclient.ListGet(self.Binname + "::" + key, list)
}

func (self *midClient) ListAppend(kv *KeyValue, succ *bool) error{
	kv.Key = self.Binname + "::" + kv.Key
	return self.Selclient.ListAppend(kv,succ)
}

func (self *midClient) ListRemove(kv *KeyValue, n *int) error{
	kv.Key = self.Binname + "::" + kv.Key
	return self.Selclient.ListRemove(kv,n)
}

func (self *midClient) ListKeys(p *Pattern, list *List) error{
    p.Prefix = self.Binname + "::" + p.Prefix
	e := self.Selclient.ListKeys(p,list)
	if e != nil{
		return e
	}
	for k := range list.L{
	  list.L[k] = strings.TrimPrefix(list.L[k], self.Binname + "::")
	}
	return nil
}

func (self *midClient) Clock(atLeast uint64, ret *uint64) error{
	return self.Selclient.Clock(atLeast, ret)
}
