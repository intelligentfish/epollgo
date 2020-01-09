package main

import (
	"flag"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/intelligentfish/gogo/byte_buf"
)

func main() {
	flag.Parse()
	flag.Set("v", "0")
	flag.Set("logtostderr", "true")

	wg := sync.WaitGroup{}
	count := 100
	remain := int32(0)
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(id int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", "127.0.0.1:10080")
			if nil != err {
				glog.Error(err)
				return
			}

			defer atomic.AddInt32(&remain, -1)
			glog.Info("total count: ", atomic.AddInt32(&remain, 1))

			buf := byte_buf.GetPoolInstance().Borrow(byte_buf.InitCapOption(16))
			for j := 0; j < 10; j++ {
				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if _, err = conn.Write([]byte(fmt.Sprintf("(%d,%d)", id, j))); nil != err {
					glog.Error(err)
					break
				}

				var n int
				conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				if n, err = conn.Read(buf.Internal()[buf.WriterIndex():buf.InitCap()]); nil != err {
					glog.Error(err)
					break
				}

				// 假设数据量很小，一次就能读完
				if 0 < n {
					buf.SetWriterIndex(buf.WriterIndex() + n)
					//glog.Info(string(buf.Internal()[buf.ReaderIndex():buf.WriterIndex()]))
				}

				time.Sleep(time.Second)
			}
			glog.Infof("%d done", id)
			conn.Close()
			byte_buf.GetPoolInstance().Return(buf.DiscardAllBytes())
		}(i)
	}
	go func() {
		for {
			glog.Infof("remain: %d", atomic.LoadInt32(&remain))
			time.Sleep(time.Second)
		}
	}()
	wg.Wait()
	glog.Info("event loop client done")
}
