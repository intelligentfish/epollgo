// +build linux

package epollgo

import (
	"github.com/golang/glog"
	"golang.org/x/sys/unix"
	"net"
	"sync"
	"sync/atomic"
)

// 全局变量
var (
	nextEventLoopID int32 // 下一个事件循环id
)

// CtxFactory Ctx工厂
type CtxFactory func(eventLoop *EventLoop /*事件循环*/) *Ctx

// acceptParam
type acceptParam struct {
	fd   int           // 文件描述符
	addr unix.Sockaddr // Socket地址
}

// EventLoop 事件循环
type EventLoop struct {
	id       int32 // ID
	isMaster bool  // 是否主节点

	// 为了让更多的客户端连接能顺利建立，适当增大完成队列的长度
	backlog int // 完成队列大小

	// 文件描述符
	epFD int // Epoll文件描述符
	lnFD int // 监听文件描述符

	// 控制反应堆正常结束
	ctrlRPipe int // 控制管道
	ctrlWPipe int // 控制管道

	// 主从反应堆的核心
	slaveLoops     []*EventLoop // 从事件循环
	slaveLoopsLock sync.RWMutex // 从事件循环锁

	// 监视事件组，这个有点坑，和C、Epoll差异有点大，此处采用的策略是分配一个很大的数组，而不是append，否则效率极低
	// C的定义
	// typedef union epoll_data {
	//    void    *ptr;
	//    int      fd;
	//    uint32_t u32;
	//    uint64_t u64;
	// } epoll_data_t;
	//
	// struct epoll_event {
	//    uint32_t     events;    /* Epoll events */
	//    epoll_data_t data;      /* User data variable */
	//};
	// GO的定义
	// type EpollEvent struct {
	//     Events uint32
	//     Fd     int32
	//     Pad    int32
	// }
	epEvents     []unix.EpollEvent // Epoll事件组
	epEventsLock sync.RWMutex      // Epoll事件组锁

	// 轮流负载均衡策略下从事件循环的索引
	lbIndex int // 负载均衡索引

	totalFD int32 // 总持有FD数量

	// 防止多次调用Start|Stop方法的标志位，sync.Once也可以，不过显得有点浪费
	startFlag int32 // 启动标志
	stopFlag  int32 // 停止标志

	// 等待组，等待协程结束
	wg sync.WaitGroup // 等待组

	// 上下文工厂，必须提供，否则事件无法传递到上层
	ctxFactory CtxFactory // 上下文工厂

	// golang这个就有点坑了，没有和C EpllEvent的结构对应上，否则用ptr可以完美解决，还少了查找过程，这大概就是C性能更高的原因吧
	ctxMap     map[int]*Ctx // 上下文Map
	ctxMapLock sync.RWMutex // 上下文Map锁

	// 异步接受通道，让主时间循环更快地执行Accept操作，否则很多链接会在没有收到SYN ACK而连接超时
	acceptParamChan chan *acceptParam // 接受参数通道
}

// EventLoopOption 事件循环选项
type EventLoopOption func(object *EventLoop /*事件循环*/)

// EventLoopBacklogOption 完成队列长度选项
func EventLoopBacklogOption(backlog int /*完成队列长度*/) EventLoopOption {
	return func(object *EventLoop) {
		object.backlog = backlog
	}
}

// addCtx 添加上下文
func (object *EventLoop) addCtx(fd int, /*文件描述符*/
	addr unix.Sockaddr /*socket地址*/) *Ctx {
	if nil != object.ctxFactory {
		ctx := object.ctxFactory(object)
		if ctx.AcceptEvent(fd, addr) {
			// 加锁，维护fd和上下文的对应关系，如果可以不加锁该有多好
			object.ctxMapLock.Lock()
			object.ctxMap[fd] = ctx
			object.ctxMapLock.Unlock()
			return ctx
		}
		// 外部策略可控制，拒绝连接
		ctx.fd = fd
		ctx.eventIndex = -1
		object.delFD(ctx)
	}
	return nil
}

// findCtx 查找上下文
func (object *EventLoop) findCtx(fd int /*文件描述符*/) (ctx *Ctx) {
	object.ctxMapLock.RLock()
	if v, ok := object.ctxMap[fd]; ok {
		ctx = v
	}
	object.ctxMapLock.RUnlock()
	return
}

// unsafeDelCtx 非安全删除上下文
func (object *EventLoop) unsafeDelCtx(ctx *Ctx /*上下文*/) {
	if _, ok := object.ctxMap[ctx.fd]; ok {
		//glog.Info("unsafeDelCtx: ", ctx.fd, ",", ctx.eventIndex)
		delete(object.ctxMap, ctx.fd)
	} else {
		glog.Errorf("unsafeDelCtx not exists fd: %d", ctx.fd)
	}
	return
}

// asyncAccept 异步接受
func (object *EventLoop) asyncAccept(fd int, /*文件描述符*/
	addr unix.Sockaddr /*socket地址*/) *EventLoop {
	object.acceptParamChan <- &acceptParam{
		fd:   fd,
		addr: addr,
	}
	return object
}

// asyncHandleAccept 异步处理接受
func (object *EventLoop) asyncHandleAccept() {
	defer object.wg.Done()
	for param := range object.acceptParamChan {
		object.accept(param.fd, param.addr)
	}
}

// accept 接受
func (object *EventLoop) accept(fd int, /*文件描述符*/
	addr unix.Sockaddr /*socket地址*/) *EventLoop {
	// 产生上下文
	ctx := object.addCtx(fd, addr)
	if nil == ctx {
		return object
	}
	// 使fd非阻塞
	err := object.makeFDNonBlock(fd)
	if nil != err {
		glog.Error(err)
		return object
	}
	// 使fd可读
	if err = object.makeFDReadable(ctx, true, true); nil != err {
		glog.Error(err)
		return object
	}
	// 外部保存时间索引，省去遍历查找的开销
	return object
}

// dispatch 派发
func (object *EventLoop) dispatch(fd int, /*文件描述符*/
	addr unix.Sockaddr /*socket地址*/) *EventLoop {
	// 轮流负载均衡算法
	object.slaveLoops[object.lbIndex].accept(fd, addr)
	object.lbIndex = (object.lbIndex + 1) % len(object.slaveLoops)
	return object
}

// addEvent 添加事件
func (object *EventLoop) addEvent(ctx *Ctx, /*上下文*/
	events uint32 /*事件*/) {
	object.epEventsLock.Lock()
	index := 0
	for ; index < len(object.epEvents); index++ {
		if 0 == object.epEvents[index].Fd {
			// 查找空位
			break
		}
	}
	if index >= len(object.epEvents) {
		// 扩容
		t := make([]unix.EpollEvent, 2*cap(object.epEvents))
		copy(t, object.epEvents)
		object.epEvents = t
	}
	// 返回索引
	ctx.eventIndex = index
	object.epEvents[index].Fd = int32(ctx.fd) // 填入fd
	object.epEvents[index].Events = events    // 填入事件
	object.epEventsLock.Unlock()

	// 维护总持有文件描述符数量
	atomic.AddInt32(&object.totalFD, 1)
	return
}

// modEvent 修改事件
func (object *EventLoop) modEvent(ctx *Ctx, /*上下文*/
	events uint32 /*事件*/) {
	object.epEventsLock.Lock()
	object.epEvents[ctx.eventIndex].Events = events
	object.epEventsLock.Unlock()
	return
}

// unsafeDelEvent 非安全删除事件
func (object *EventLoop) unsafeDelEvent(ctx *Ctx /*上下文*/) *EventLoop {
	if -1 == ctx.eventIndex {
		return object
	}
	//glog.Info("unsafeDelEvent: ", ctx.fd, ",", ctx.eventIndex)
	if 0 != ctx.eventIndex {
		object.epEvents[ctx.eventIndex].Fd = 0
	} else {
		i := 0
		for ; i < len(object.epEvents); i++ {
			if int32(ctx.fd) == object.epEvents[i].Fd {
				object.epEvents[i].Fd = 0
				break
			}
		}
		if i >= len(object.epEvents) {
			glog.Errorf("unsafeDelEvent fd: %d not exists", ctx.fd)
		}
	}
	atomic.AddInt32(&object.totalFD, -1)
	return object
}

// isErrorEvent 是否错误事件
func (object *EventLoop) isErrorEvent(e *unix.EpollEvent /*Epoll事件*/) bool {
	return 0 != e.Events&unix.EPOLLERR ||
		0 != e.Events&unix.EPOLLRDHUP
}

// isReadEvent 会否读事件
func (object *EventLoop) isReadEvent(e *unix.EpollEvent /*Epoll事件*/) bool {
	return 0 != e.Events&unix.EPOLLIN
}

// isWriteEvent 是否写事件
func (object *EventLoop) isWriteEvent(e *unix.EpollEvent /*Epoll事件*/) bool {
	return 0 != e.Events&unix.EPOLLOUT
}

// makeFDNonBlock 使FD非阻塞
func (object *EventLoop) makeFDNonBlock(fd int /*文件描述符*/) (err error) {
	err = unix.SetNonblock(fd, true)
	return
}

// makeSocketReuseAddr 使Socket重用地址
func (object *EventLoop) makeSocketReuseAddr(fd int /*文件描述符*/) (err error) {
	err = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
	return
}

// makeSocketReusePort 使Socket重用端口
func (object *EventLoop) makeSocketReusePort(fd int /*文件描述符*/) (err error) {
	err = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
	return
}

// ctlFD 控制FD
func (object *EventLoop) ctrlFD(ctx *Ctx, /*上下文*/
	addOrMod bool, /*添加或修改*/
	events uint32, /*事件*/
) (err error) {
	e := unix.EpollEvent{
		Events: events,
		Fd:     int32(ctx.fd),
	}
	op := unix.EPOLL_CTL_ADD
	if !addOrMod {
		op = unix.EPOLL_CTL_MOD
	}
	if addOrMod {
		object.addEvent(ctx, e.Events)
	} else {
		object.modEvent(ctx, e.Events)
	}
	err = unix.EpollCtl(object.epFD, op, ctx.fd, &e)
	return
}

// makeFDReadable 使FD可读
func (object *EventLoop) makeFDReadable(ctx *Ctx, /*上下文*/
	addOrMod /*添加或修改*/, oneShot /*单次生效*/ bool) (err error) {
	events := uint32(unix.EPOLLET | unix.EPOLLIN)
	if oneShot {
		events |= unix.EPOLLONESHOT
	}
	err = object.ctrlFD(ctx, addOrMod, events)
	return
}

// makeFDWriteable 使FD可写
func (object *EventLoop) makeFDWriteable(ctx *Ctx, /*文件描述符*/
	addOrMod /*添加或修改*/, oneShot /*单次生效*/ bool) (err error) {
	events := uint32(unix.EPOLLET | unix.EPOLLOUT)
	if oneShot {
		events |= unix.EPOLLONESHOT
	}
	err = object.ctrlFD(ctx, addOrMod, events)
	return
}

// makeFDReadWriteable 使FD可写
func (object *EventLoop) makeFDReadWriteable(ctx *Ctx, /*上下文*/
	addOrMod /*添加或修改*/, oneShot /*单次生效*/ bool) (err error) {
	events := uint32(unix.EPOLLET | unix.EPOLLIN | unix.EPOLLOUT)
	if oneShot {
		events |= unix.EPOLLONESHOT
	}
	err = object.ctrlFD(ctx, addOrMod, events)
	return
}

// delFD 删除FD
func (object *EventLoop) delFD(ctx *Ctx /*上下文*/) (err error) {
	err = unix.EpollCtl(object.epFD, unix.EPOLL_CTL_DEL, ctx.fd, nil)
	object.epEventsLock.Lock()
	object.ctxMapLock.Lock()
	object.unsafeDelEvent(ctx)
	object.unsafeDelCtx(ctx)
	object.ctxMapLock.Unlock()
	object.epEventsLock.Unlock()
	return
}

// closeAllFD 关闭所有FD
func (object *EventLoop) closeAllFD() {
	object.epEventsLock.Lock()
	defer object.epEventsLock.Unlock()
	var err error
	for i := 0; i < len(object.epEvents); i++ {
		if 0 == object.epEvents[i].Fd {
			continue
		}
		if err = unix.Close(int(object.epEvents[i].Fd)); nil != err {
			glog.Error(err)
		}
		object.epEvents[i].Fd = 0
	}
}

// reactor 反应堆
func (object *EventLoop) reactor() {
	defer object.wg.Done()

	var ok = true
	var n int
	var err error
loop:
	for ok {
		// 等待事件发生
		if n, err = unix.EpollWait(object.epFD, object.epEvents, -1); nil != err {
			glog.Error(err)
		}
		// 遍历所有事件
		for i := 0; i < n; i++ {
			e := &object.epEvents[i]
			// 主动退出信号
			if int32(object.ctrlRPipe) == e.Fd {
				ok = false
				break loop
			}
			// 错误事件
			if object.isErrorEvent(e) {
				if int32(object.lnFD) == e.Fd {
					glog.Errorf("event reactor: %d, listen fd error: %d", object.id, e.Events)
					break loop
				}
				ctx := object.findCtx(int(e.Fd))
				if nil != ctx {
					object.delFD(ctx)
				}
				continue
			}
			// 主事件循环
			if object.isMaster {
				if object.isReadEvent(e) {
					var fd int
					var addr unix.Sockaddr
					for {
						// 接受连接
						fd, addr, err = unix.Accept(object.lnFD)
						if nil != err {
							if unix.EAGAIN != err {
								glog.Errorf("event reactor: %d, unix.Accept error: %s", object.id, err)
							}
							break
						}
						// 使socket地址可重用
						if err = object.makeSocketReuseAddr(fd); nil != err {
							glog.Error(err)
						}
						// 使socket端口可重用
						if err = object.makeSocketReusePort(fd); nil != err {
							glog.Error(err)
						}
						// 负载均衡到从结点
						object.dispatch(fd, addr)
					}
				}
				continue
			}
			// 从事件循环
			if object.isReadEvent(e) {
				ctx := object.findCtx(int(e.Fd))
				if nil == ctx {
					//panic(fmt.Sprintf("read event bug!!!, fd: %d ctx is nil", e.Fd))
					unix.Close(int(e.Fd))
				} else {
					ctx.ReadEvent()
				}
				continue
			}
			if object.isWriteEvent(e) {
				// 可写不做任何处理，外部多协程写
				glog.Error("event reactor: %d, write event", object.id)
				continue
			}
			glog.Errorf("event reactor: %d, unknown event: %d", object.id, e.Events)
		}
	}
	close(object.acceptParamChan)
}

// New 工厂方法
func New() (object *EventLoop, err error) {
	object = &EventLoop{}
	// 创建Epoll文件描述符
	if object.epFD, err = unix.EpollCreate1(unix.EPOLL_CLOEXEC); nil != err {
		return
	}
	// 控制管道
	ctrlPipes := make([]int, 2)
	if err = unix.Pipe(ctrlPipes); nil != err {
		return
	}
	// 增加事件循环ID
	object.id = atomic.AddInt32(&nextEventLoopID, 1)
	object.ctrlRPipe = ctrlPipes[0]                         // 控制读管道
	object.ctrlWPipe = ctrlPipes[1]                         // 控制写管道
	object.slaveLoops = make([]*EventLoop, 0)               // 从循环数组
	object.epEvents = make([]unix.EpollEvent, 1<<20)        // 初始事件容量
	object.ctxMap = make(map[int]*Ctx)                      // 上下文查找表
	object.acceptParamChan = make(chan *acceptParam, 1<<20) // 接受事件通道
	return
}

// SetCtxFactory 设置Ctx工厂
func (object *EventLoop) SetCtxFactory(factory CtxFactory /*上下文工厂*/) *EventLoop {
	object.ctxFactory = factory
	return object
}

// Listen 侦听
func (object *EventLoop) Listen(port int, /*端口*/
	options ...EventLoopOption, /*事件循环选项*/
) (err error) {
	for _, option := range options {
		option(object)
	}
	// 创建侦听socket
	if object.lnFD, err = unix.Socket(unix.AF_INET,
		unix.SOCK_STREAM|unix.O_NONBLOCK,
		unix.IPPROTO_TCP); nil != err {
		return
	}
	// 重用socket地址
	if err = object.makeSocketReuseAddr(object.lnFD); nil != err {
		return
	}
	// 重用socket端口
	if err = object.makeSocketReusePort(object.lnFD); nil != err {
		return
	}
	// 绑定地址
	addr := &unix.SockaddrInet4{Port: port}
	copy(addr.Addr[:], net.ParseIP("0.0.0.0").To4())
	if err = unix.Bind(object.lnFD, addr); nil != err {
		return
	}
	// 完成队列长度
	if 0 >= object.backlog {
		object.backlog = 128
	}
	if err = unix.Listen(object.lnFD, object.backlog); nil != err {
		return
	}
	// 开启Epoll侦听读事件
	ctx := &Ctx{fd: object.lnFD}
	err = object.makeFDReadable(ctx, true, false)
	return
}

// Group 组合
func (object *EventLoop) Group(slave *EventLoop /*事件循环*/) *EventLoop {
	object.slaveLoopsLock.Lock()
	defer object.slaveLoopsLock.Unlock()
	object.isMaster = true
	object.slaveLoops = append(object.slaveLoops, slave)
	return object
}

// Start 启动
func (object *EventLoop) Start() (err error) {
	if !atomic.CompareAndSwapInt32(&object.startFlag, 0, 1) {
		return
	}
	// 非阻塞
	if err = object.makeFDNonBlock(object.ctrlRPipe); nil != err {
		return
	}
	// 非阻塞
	if err = object.makeFDNonBlock(object.ctrlWPipe); nil != err {
		return
	}
	// Epoll侦听读事件
	ctx := &Ctx{fd: object.ctrlRPipe}
	if err = object.makeFDReadable(ctx, true, false); nil != err {
		return
	}
	object.wg.Add(2)
	go object.reactor()
	go object.asyncHandleAccept()
	return
}

// Stop 停止
func (object *EventLoop) Stop() {
	if !atomic.CompareAndSwapInt32(&object.stopFlag, 0, 1) {
		return
	}
	// 停止监听
	var err error
	if 0 < object.lnFD {
		if err = unix.Close(object.lnFD); nil != err {
			glog.Error(err)
		}
	}
	// 终止Epoll循环
	if 0 < object.ctrlWPipe {
		if err = unix.Close(object.ctrlWPipe); nil != err {
			glog.Error(err)
		}
	}
	object.wg.Wait()
	// TODO 等待所有处理完成
	if !object.isMaster {
		object.closeAllFD()
	}
	// 关闭Epoll文件描述符
	if err = unix.Close(object.epFD); nil != err {
		glog.Error(err)
	}
}
