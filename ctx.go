package epollgo

import (
	"context"
	"net"
	"sync/atomic"

	"github.com/intelligentfish/gogo/byte_buf"
	"github.com/intelligentfish/gogo/routine_pool"
	"golang.org/x/sys/unix"
)

// AcceptEventHook 接受事件钩子
type AcceptEventHook func() bool

// ReadEventHook 读事件钩子
type ReadEventHook func(buf *byte_buf.ByteBuf, err error)

// WriteEventHook 写事件钩子
type WriteEventHook func(buf *byte_buf.ByteBuf, err error)

// Ctx 处理器
type Ctx struct {
	eventIndex      int                 // 事件索引
	eventLoop       *EventLoop          // 关联的事件循环
	fd              int                 // 文件描述符
	addr            *unix.SockaddrInet4 // 原始地址
	v4ip            net.IP              // IP地址
	port            int                 // 端口
	readEndFlag     int32               // 读完成标志
	writeEndFlag    int32               // 写完成标志
	acceptEventHook AcceptEventHook     // 接受钩子
	readEventHook   ReadEventHook       // 读事件钩子
	writeEventHook  WriteEventHook      // 写事件钩子
	readBufferSize  int                 // 读缓冲区大小
}

// CtxOption 上下文选项
type CtxOption func(ctx *Ctx)

// CtxBufferSizeOption 缓冲区大小选项
func CtxBufferSizeOption(size int) CtxOption {
	return func(ctx *Ctx) {
		ctx.readBufferSize = size
	}
}

// CtxEventLoopOption EventLoop选项
func CtxEventLoopOption(eventLoop *EventLoop) CtxOption {
	return func(ctx *Ctx) {
		ctx.eventLoop = eventLoop
	}
}

// CtxAcceptEventHookOption 接受事件钩子选项
func CtxAcceptEventHookOption(hook AcceptEventHook) CtxOption {
	return func(ctx *Ctx) {
		ctx.acceptEventHook = hook
	}
}

// CtxReadEventHookOption 读取事件钩子选项
func CtxReadEventHookOption(hook ReadEventHook) CtxOption {
	return func(ctx *Ctx) {
		ctx.readEventHook = hook
	}
}

// CtxWriteEventHookOption 写事件钩子选项
func CtxWriteEventHookOption(hook WriteEventHook) CtxOption {
	return func(ctx *Ctx) {
		ctx.writeEventHook = hook
	}
}

// shutdownSocket 关闭socket
func (object *Ctx) shutdownSocket(read bool) {
	if read {
		// 关闭Socket读
		unix.Shutdown(object.fd, unix.SHUT_RD)
	}
	if !read {
		// 关闭Socket写
		unix.Shutdown(object.fd, unix.SHUT_WR)
	}
	// 读写都已关闭，关闭连接
	if 1 == atomic.LoadInt32(&object.writeEndFlag) &&
		1 == atomic.LoadInt32(&object.readEndFlag) {
		object.Close()
	}
}

// NewCtx 工厂方法
func NewCtx(options ...CtxOption) *Ctx {
	object := &Ctx{
		readEndFlag:    1,       // 默认读完成
		writeEndFlag:   1,       // 默认写完成
		readBufferSize: 1 << 13, // 默认缓冲区大小
	}
	for _, option := range options {
		option(object)
	}
	return object
}

// SetOption 设置可选项
func (object *Ctx) SetOption(options ...CtxOption) *Ctx {
	for _, option := range options {
		option(object)
	}
	return object
}

// GetV4IP 获取IP地址
func (object *Ctx) GetV4IP() string {
	return object.v4ip.String()
}

// GetPort 获取端口
func (object *Ctx) GetPort() int {
	return object.addr.Port
}

// Close 关闭
func (object *Ctx) Close() {
	// glog.Infof("Close: (%d,%s:%d)",
	// 	object.eventLoop.id,
	// 	object.GetV4IP(),
	// 	object.GetPort())
	object.eventLoop.delFD(object.fd, object.eventIndex)
}

// AcceptEvent 接受
func (object *Ctx) AcceptEvent(fd int, addr unix.Sockaddr) bool {
	object.fd = fd
	object.addr = addr.(*unix.SockaddrInet4)
	object.v4ip = net.IPv4(object.addr.Addr[0],
		object.addr.Addr[1],
		object.addr.Addr[2],
		object.addr.Addr[3]).To4()
	// glog.Infof("AcceptEvent: (%d,%s:%d)",
	// 	object.eventLoop.id,
	// 	object.GetV4IP(),
	// 	object.GetPort())
	if nil != object.acceptEventHook {
		return object.acceptEventHook()
	}
	return true
}

// ReadEvent 处理读
func (object *Ctx) ReadEvent() {
	atomic.StoreInt32(&object.readEndFlag, 0) // 重置读完成标志
	// 提交读任务
	routine_pool.GetInstance().CommitTask(func(ctx context.Context, params []interface{}) {
		var n int
		var err error
		buf := byte_buf.GetPoolInstance().Borrow(byte_buf.InitCapOption(object.readBufferSize))
		for {
			atomic.StoreInt32(&object.readEndFlag, 0)                                      // 重置读完成标志
			n, err = unix.Read(object.fd, buf.Internal()[buf.WriterIndex():buf.InitCap()]) // 读
			atomic.StoreInt32(&object.readEndFlag, 1)                                      // 设置读完成标志
			if nil != err {
				// 内核没有数据可读，下一次继续
				if unix.EAGAIN == err {
					_, err = object.eventLoop.makeFDReadable(int32(object.fd),
						object.eventIndex,
						false,
						true)
				}
				break
			}
			if 0 == n {
				// 远程关闭了读或连接已断开
				object.shutdownSocket(true)
				break
			}
			// 设置写索引
			buf.SetWriterIndex(buf.WriterIndex() + n)
		}
		// 回调读事件
		object.readEventHook(buf, err)
	}, "CtxRead")
}

// WriteEvent 处理写
func (object *Ctx) WriteEvent(buf *byte_buf.ByteBuf, err error) {
	// 回调写事件
	if nil != object.writeEventHook {
		object.writeEventHook(buf, err)
	}
}

// Write 异步写
func (object *Ctx) Write(buf *byte_buf.ByteBuf) {
	atomic.StoreInt32(&object.writeEndFlag, 0) // 重置读结束标志
	// 提交写任务
	routine_pool.GetInstance().CommitTask(func(ctx context.Context, params []interface{}) {
		var n int
		var err error
		for buf.IsReadable() {
			atomic.StoreInt32(&object.writeEndFlag, 0)                                          // 重置读结束标志
			n, err = unix.Write(object.fd, buf.Internal()[buf.ReaderIndex():buf.WriterIndex()]) // 写
			atomic.StoreInt32(&object.writeEndFlag, 1)                                          // 设置读结束标志
			if nil != err {
				// 内核写缓冲区已满，下一次继续
				if unix.EAGAIN == err {
					_, err = object.eventLoop.makeFDWriteable(int32(object.fd),
						object.eventIndex,
						false,
						true)
				}
				break
			}
			if 0 == n {
				// 写结束
				object.shutdownSocket(false)
				break
			}
			// 设置读索引
			buf.SetReaderIndex(buf.ReaderIndex() + n)
		}
		// 回调事件
		object.writeEventHook(buf, nil)
	}, "CtxWrite")
}
