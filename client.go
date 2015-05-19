package triblab

import (
	"net/rpc"
   	. "trib"
   	"sync"
)


type client struct{
	addr string
    //a component to memorize the existing connect
	conn *rpc.Client
	lock sync.Mutex
}

//the function used for reconnecting
func (self *client) reconn(num int) (*rpc.Client,error){
	var e error
	var newconn *rpc.Client
	for i := 0; i < num; i++{
		newconn, e = rpc.DialHTTP("tcp", self.addr)
		if e == nil {
			break
		}
	}
	if e != nil{
		return nil, e
	}
	return newconn, e
}


/*
the eight functions are almost the same, detect if there's connect
first and decide if need to create a new connect. then use the 
connect to call the specific function and return the error var.
this version is somewhat based on the example posted online
*/
func (self *client) Get(key string, value *string) error{
	//an error parameter to be used
	var e error
	var newconn *rpc.Client

	if self.conn == nil {
		newconn, e = self.reconn(5)
		if e != nil{
			return e
		}
	}

	self.lock.Lock()
	if self.conn == nil{
		self.conn = newconn
	}
	self.lock.Unlock()

	e = self.conn.Call("Storage.Get", key, value)
	if e != nil {
		//reconnect 2 times if shutdown
		if e == rpc.ErrShutdown{
			newconn, e = self.reconn(2)

			if e != nil{
				return e
			}

			self.conn = newconn

			e = self.conn.Call("Storage.Get", key, value)
			if e != nil{
				self.conn.Close()
				return e
			}
		} else{
			self.conn.Close()
			return e
		}
	}

	return nil
}

func (self *client) Set(kv *KeyValue, succ *bool) error{
	//an error parameter to be used
	var e error
	var newconn *rpc.Client

	if self.conn == nil {
		newconn, e = self.reconn(5)
		if e != nil{
			return e
		}
	}

	self.lock.Lock()
	if self.conn == nil{
		self.conn = newconn
	}
	self.lock.Unlock()

	e = self.conn.Call("Storage.Set", kv, succ)
	if e != nil {
		if e == rpc.ErrShutdown{
			newconn, e = self.reconn(2)

			if e != nil{
				return e
			}

			self.conn = newconn

			e = self.conn.Call("Storage.Set", kv, succ)
			if e != nil{
				self.conn.Close()
				return e
			}
		} else{
			self.conn.Close()
			return e
		}
	}

	return nil
	
}

func (self *client) Keys(p *Pattern, list *List) error{
	//an error parameter to be used
	var e error
	var newconn *rpc.Client
	
	if self.conn == nil {
		newconn, e = self.reconn(5)
		if e != nil{
			return e
		}
	}

	self.lock.Lock()
	if self.conn == nil{
		self.conn = newconn
	}
	self.lock.Unlock()

	list.L = []string{}
	e = self.conn.Call("Storage.Keys", p, list)
	if e != nil {
		if e == rpc.ErrShutdown{
			newconn, e = self.reconn(2)

			if e != nil{
				return e
			}

			self.conn = newconn

			e = self.conn.Call("Storage.Keys", p, list)
			if e != nil{
				self.conn.Close()
				return e
			}
		} else{
			self.conn.Close()
			return e
		}
	}

	return nil
}

func (self *client) ListGet(key string, list *List) error{
	//an error parameter to be used
	var e error
	var newconn *rpc.Client

	if self.conn == nil {
		newconn, e = self.reconn(5)
		if e != nil{
			return e
		}
	}

	self.lock.Lock()
	if self.conn == nil{
		self.conn = newconn
	}
	self.lock.Unlock()

	//set the list to an empty string array before call
	list.L = []string{}
	e = self.conn.Call("Storage.ListGet", key, list)
	//if the list is nil it should be viewed as an error
	if e != nil {
		if e == rpc.ErrShutdown{
			newconn, e = self.reconn(2)

			if e != nil{
				return e
			}

			self.conn = newconn

			e = self.conn.Call("Storage.ListGet", key, list)
			if e != nil{
				self.conn.Close()
				return e
			}
		} else{
			self.conn.Close()
			return e
		}
	}

	return nil

}

func (self *client) ListAppend(kv *KeyValue, succ *bool) error{
	//an error parameter to be used
	var e error
	var newconn *rpc.Client

	if self.conn == nil {
		newconn, e = self.reconn(5)
		if e != nil{
			return e
		}
	}

	self.lock.Lock()
	if self.conn == nil{
		self.conn = newconn
	}
	self.lock.Unlock()

	e = self.conn.Call("Storage.ListAppend", kv, succ)
	if e != nil {
		if e == rpc.ErrShutdown{
			newconn, e = self.reconn(2)

			if e != nil{
				return e
			}

			self.conn = newconn

			e = self.conn.Call("Storage.ListAppend", kv, succ)
			if e != nil{
				self.conn.Close()
				return e
			}
		} else{
			self.conn.Close()
			return e
		}
	}

	return nil

}

func (self *client) ListRemove(kv *KeyValue, n *int) error{
	//an error parameter to be used
	var e error
	var newconn *rpc.Client

	if self.conn == nil {
		newconn, e = self.reconn(5)
		if e != nil{
			return e
		}
	}

	self.lock.Lock()
	if self.conn == nil{
		self.conn = newconn
	}
	self.lock.Unlock()

	e = self.conn.Call("Storage.ListRemove", kv, n)
	if e != nil {
		if e == rpc.ErrShutdown{
			newconn, e = self.reconn(2)

			if e != nil{
				return e
			}

			self.conn = newconn

			e = self.conn.Call("Storage.ListRemove", kv, n)
			if e != nil{
				self.conn.Close()
				return e
			}
		} else{
			self.conn.Close()
			return e
		}
	}

	return nil

}

func (self *client) ListKeys(p *Pattern, list *List) error{
	//an error parameter to be used
	var e error
	var newconn *rpc.Client

	if self.conn == nil {
		newconn, e = self.reconn(5)
		if e != nil{
			return e
		}
	}

	self.lock.Lock()
	if self.conn == nil{
		self.conn = newconn
	}
	self.lock.Unlock()

	list.L = []string{}
	e = self.conn.Call("Storage.ListKeys", p, list)
	if e != nil {
		if e == rpc.ErrShutdown{
			newconn, e = self.reconn(2)

			if e != nil{
				return e
			}

			self.conn = newconn

			if e != nil{
				self.conn.Close()
				return e
			}
			e = self.conn.Call("Storage.ListKeys", p, list)
			if e != nil{
				self.conn.Close()
				return e
			}
		} else{
			self.conn.Close()
			return e
		}
	}

	return nil

}

func (self *client) Clock(atLeast uint64, ret *uint64) error{
	//an error parameter to be used
	var e error
	var newconn *rpc.Client
	
	if self.conn == nil {
		newconn, e = self.reconn(5)
		if e != nil{
			return e
		}
	}

	self.lock.Lock()
	if self.conn == nil{
		self.conn = newconn
	}
	self.lock.Unlock()
	
	e = self.conn.Call("Storage.Clock", atLeast, ret)
	if e != nil {
		if e == rpc.ErrShutdown{
			newconn, e = self.reconn(2)

			if e != nil{
				return e
			}

			self.conn = newconn

			if e != nil{
				self.conn.Close()
				return e
			}
			e = self.conn.Call("Storage.Clock", atLeast, ret)
			if e != nil{
				self.conn.Close()
				return e
			}
		} else{
			self.conn.Close()
			return e
		}
	}

	return nil
}
