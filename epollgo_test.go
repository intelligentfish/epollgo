// +build linux

package epollgo

import (
	"context"
	"flag"
	"net/http"
	"reflect"
	"testing"

	"github.com/golang/glog"
	"github.com/intelligentfish/gogo/app"
	"github.com/intelligentfish/gogo/byte_buf"
	"github.com/intelligentfish/gogo/event"
	"github.com/intelligentfish/gogo/event_bus"
	"github.com/intelligentfish/gogo/priority_define"
	"github.com/intelligentfish/gogo/routine_pool"
)

func main() {
	flag.Parse()
	flag.Set("v", "0")
	flag.Set("logtostderr", "true")

	go func() {
		err := http.ListenAndServe(":10081", nil)
		if nil != err {
			glog.Error(err)
		}
	}()

	// 初始化Master
	master, err := New()
	if nil != err {
		glog.Error(err)
		return
	}

	// Master侦听
	if err = master.Listen(10080, EventLoopBacklogOption(1<<20)); nil != err {
		glog.Error(err)
		return
	}

	// 初始化Slave
	var slaveLoops []*EventLoop
	for i := 0; i < 12; i++ {
		loop, err := New()
		if nil != err {
			glog.Error(err)
			return
		}
		slaveLoops = append(slaveLoops, loop)
	}

	// 组合主从EventLoop
	for i := 0; i < len(slaveLoops); i++ {
		master.Group(slaveLoops[i])
	}

	// 设置工厂
	for _, slaveLoop := range slaveLoops {
		slaveLoop.SetCtxFactory(func(eventLoop *EventLoop) *Ctx {
			ctx := NewCtx(CtxEventLoopOption(eventLoop), CtxBufferSizeOption(1<<12))
			ctx.SetOption(CtxAcceptEventHookOption(func() bool {
				//glog.Info("ACCEPT [")
				//glog.Infof("(%s:%d)", ctx.GetV4IP(), ctx.GetPort())
				//glog.Info("] ACCEPT")
				return true
			})).SetOption(CtxReadEventHookOption(func(buf *byte_buf.ByteBuf, err error) {
				//glog.Info("READ [")
				//glog.Info(string(buf.Internal()[buf.ReaderIndex():buf.WriterIndex()]))
				//glog.Info("] READ")

				if nil == err {
					ctx.Write(buf)
					return
				}
				glog.Error("READ error: ", err)
				ctx.Close()
			})).SetOption(CtxWriteEventHookOption(func(buf *byte_buf.ByteBuf, err error) {
				if nil != err {
					glog.Error("WRITE error: ", err)
					ctx.Close()
					return
				}
				if buf.IsReadable() {
					ctx.Write(buf)
				} else {
					// 退还资源
					byte_buf.GetPoolInstance().Return(buf)
				}
			}))
			return ctx
		})
	}

	// 启动EventLoop
	if err = master.Start(); nil != err {
		glog.Error(err)
		return
	}

	// 启动EventLoop
	for _, slaveLoop := range slaveLoops {
		if err = slaveLoop.Start(); nil != err {
			glog.Error(err)
			return
		}
	}

	// 等待关闭信号
	event_bus.GetInstance().Mounting(reflect.TypeOf(&event.AppShutdownEvent{}),
		func(ctx context.Context, param interface{}) {
			if priority_define.TCPServiceShutdownPriority !=
				param.(*event.AppShutdownEvent).ShutdownPriority {
				return
			}
			glog.Info("stop master loop")
			master.Stop()
			glog.Info("stop all slave loop")
			for _, slaveLoop := range slaveLoops {
				slaveLoop.Stop()
			}
		})

	// 等待进程退出
	app.GetInstance().
		AddShutdownHook(event_bus.GetInstance().NotifyAllComponentShutdown,
			event_bus.GetInstance().Stop,
			routine_pool.GetInstance().Stop).
		WaitShutdown()

	// 对比fasthttp
	//fasthttp.ListenAndServe(":10080", func(ctx *fasthttp.RequestCtx) {
	//	ctx.Write(ctx.PostBody())
	//})
}

func TestEpollGo(t *testing.T) {
	main()
}
