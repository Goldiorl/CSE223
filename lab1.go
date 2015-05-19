package triblab
//import some needed packets
import (
	"trib"
	"net"
	"net/rpc"
	"net/http"
)

// Creates an RPC client that connects to addr.
func NewClient(addr string) trib.Storage {
//return a reference of client with a specific addr
	return &client{addr: addr, conn: nil}
}

// Serve as a backend based on the given configuration
func ServeBack(b *trib.BackConfig) error {
	var l net.Listener
	var e error
//create a new server and then register it
	var ser = rpc.NewServer()
	e = ser.Register(b.Store)
	if e != nil {
		b.Ready <- false
		return e
	}
//listen to the b's addr
	l, e = net.Listen("tcp", b.Addr)
//detect if there's error and set the ready domain
	if e != nil {
		b.Ready <- false
		return e
	}
//create a new thread to do a http serve
	if b.Ready != nil {
		go func(ch chan<- bool){
			ch <- true
		}(b.Ready)
	}
	return http.Serve(l, ser)
}
