package main

import (
	"context"
	"flag"
	"github.com/intelligentfish/gogo/network/epollgo"
	"net/http"
	"reflect"
	"runtime"
	"sync/atomic"

	"github.com/golang/glog"
	"github.com/intelligentfish/gogo/app"
	"github.com/intelligentfish/gogo/byte_buf"
	"github.com/intelligentfish/gogo/event"
	"github.com/intelligentfish/gogo/event_bus"
	"github.com/intelligentfish/gogo/priority_define"
	"github.com/intelligentfish/gogo/routine_pool"
)

const (
	version = "0.0.0.3"
)

var (
	nextCtxID = int32(0)
)

func main() {
	flag.Parse()
	flag.Set("v", "0")
	flag.Set("logtostderr", "true")

	glog.Info("version: ", version)

	go func() {
		err := http.ListenAndServe(":10081", nil)
		if nil != err {
			glog.Error(err)
		}
	}()

	// 初始化Master
	master, err := epollgo.New()
	if nil != err {
		glog.Error(err)
		return
	}

	// Master侦听
	if err = master.Listen(10080, epollgo.EventLoopBacklogOption(1<<20)); nil != err {
		glog.Error(err)
		return
	}

	// 初始化Slave
	var slaveLoops []*epollgo.EventLoop
	for i := 0; i < runtime.NumCPU(); i++ {
		loop, err := epollgo.New()
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
		slaveLoop.SetCtxFactory(func(eventLoop *epollgo.EventLoop) *epollgo.Ctx {
			ctx := epollgo.NewCtx(epollgo.CtxIDOption(atomic.AddInt32(&nextCtxID, 1)),
				epollgo.CtxEventLoopOption(eventLoop),
				epollgo.CtxBufferSizeOption(64))
			ctx.SetOption(epollgo.CtxAcceptEventHookOption(func() bool {
				//glog.Info("ACCEPT [")
				//glog.Infof("(%s:%d)", ctx.GetV4IP(), ctx.GetPort())
				//glog.Info("] ACCEPT")
				return true
			})).SetOption(epollgo.CtxReadEventHookOption(func(buf *byte_buf.ByteBuf, err error) {
				if nil == err {
					// 有数据
					if buf.IsReadable() {
						// 写已关闭
						if ctx.IsWriteShutdown() {
							ctx.ShutdownSocket(true)
						} else {
							// 写数据
							ctx.Write(buf)
						}
					} else {
						// 读关闭
						ctx.ShutdownSocket(true)
					}
				} else {
					// 读关闭
					glog.Error("read error: ", err)
					ctx.ShutdownSocket(true)
				}
			})).SetOption(epollgo.CtxWriteEventHookOption(func(buf *byte_buf.ByteBuf, err error) {
				if nil != err {
					glog.Error("write error: ", err)
					// 关闭写
					ctx.ShutdownSocket(false)
					// 退还资源
					buf.DiscardAllBytes()
					byte_buf.GetPoolInstance().Return(buf)
					return
				}
				if buf.IsReadable() {
					ctx.Write(buf)
				} else {
					// 读已关闭
					if ctx.IsReadShutdown() {
						ctx.ShutdownSocket(false)
					}
					// 退还资源
					buf.DiscardAllBytes()
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
			//glog.Info("stop master loop")
			master.Stop()
			//glog.Info("stop all slave loop")
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
