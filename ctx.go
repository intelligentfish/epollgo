package epollgo

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/intelligentfish/gogo/byte_buf"
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
	sync.RWMutex
	id                int32               // 上下文id
	eventIndex        int                 // 事件索引
	eventLoop         *EventLoop          // 关联的事件循环
	fd                int                 // 文件描述符
	addr              *unix.SockaddrInet4 // 原始地址
	v4ip              net.IP              // IP地址
	port              int                 // 端口
	readShutdownFlag  int32
	writeShutdownFlag int32
	acceptEventHook   AcceptEventHook // 接受钩子
	readEventHook     ReadEventHook   // 读事件钩子
	writeEventHook    WriteEventHook  // 写事件钩子
	readBufferSize    int             // 读缓冲区大小
}

// CtxOption 上下文选项
type CtxOption func(ctx *Ctx)

// CtxIDOption ID选项
func CtxIDOption(id int32) CtxOption {
	return func(ctx *Ctx) {
		ctx.id = id
	}
}

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

// IsReadShutdown 是否读已关闭
func (object *Ctx) IsReadShutdown() bool {
	return 1 == atomic.LoadInt32(&object.readShutdownFlag)
}

// IsShutdownWrite 是否写已关闭
func (object *Ctx) IsWriteShutdown() bool {
	return 1 == atomic.LoadInt32(&object.writeShutdownFlag)
}

// ShutdownSocket 关闭socket
func (object *Ctx) ShutdownSocket(readOrWrite bool) {
	object.Lock()
	defer object.Unlock()

	if readOrWrite && atomic.CompareAndSwapInt32(&object.readShutdownFlag, 0, 1) {
		// 关闭Socket读
		unix.Shutdown(object.fd, unix.SHUT_RD)
	} else if atomic.CompareAndSwapInt32(&object.writeShutdownFlag, 0, 1) {
		// 关闭Socket写
		unix.Shutdown(object.fd, unix.SHUT_WR)
	}
	// 读写都已关闭，关闭连接
	if 1 == atomic.LoadInt32(&object.readShutdownFlag) &&
		1 == atomic.LoadInt32(&object.writeShutdownFlag) {
		object.Close()
	}
}

// NewCtx 工厂方法
func NewCtx(options ...CtxOption) *Ctx {
	object := &Ctx{
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

// GetID 获取ID
func (object *Ctx) GetID() int32 {
	return object.id
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
	object.eventLoop.delFD(object)
}

// AcceptEvent 接受
func (object *Ctx) AcceptEvent(fd int, addr unix.Sockaddr) bool {
	object.fd = fd
	object.addr = addr.(*unix.SockaddrInet4)
	object.v4ip = net.IPv4(object.addr.Addr[0],
		object.addr.Addr[1],
		object.addr.Addr[2],
		object.addr.Addr[3]).To4()

	if nil != object.acceptEventHook {
		return object.acceptEventHook()
	}
	return true
}

// ReadEvent 处理读
func (object *Ctx) ReadEvent() {
	// 提交读任务
	go func() {
		//routine_pool.GetInstance().CommitTask(func(ctx context.Context, params []interface{}) {
		var n int
		var err error
		buf := byte_buf.GetPoolInstance().Borrow(byte_buf.InitCapOption(object.readBufferSize))
		for {
			n, err = unix.Read(object.fd, buf.Internal()[buf.WriterIndex():buf.InitCap()]) // 读
			if nil != err {
				// 内核没有数据可读，下一次继续
				if unix.EAGAIN == err {
					err = object.eventLoop.makeFDReadable(object, false, true)
				}
				break
			}
			if 0 == n {
				break
			}
			// 设置写索引
			if 0 < n {
				buf.SetWriterIndex(buf.WriterIndex() + n)
			}
		}
		// 回调读事件
		object.readEventHook(buf, err)
		//}, "CtxRead")
	}()
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
	// 提交写任务
	go func() {
		//routine_pool.GetInstance().CommitTask(func(ctx context.Context, params []interface{}) {
		var n int
		var err error
		for buf.IsReadable() {
			n, err = unix.Write(object.fd, buf.Internal()[buf.ReaderIndex():buf.WriterIndex()]) // 写
			if nil != err {
				// 内核写缓冲区已满，下一次继续
				if unix.EAGAIN == err {
					err = object.eventLoop.makeFDWriteable(object, false, true)
				}
				break
			}
			if 0 == n {
				break
			}
			// 设置读索引
			buf.SetReaderIndex(buf.ReaderIndex() + n)
		}
		// 回调事件
		object.writeEventHook(buf, err)
		//}, "CtxWrite")
	}()
}
