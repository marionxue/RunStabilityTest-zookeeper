package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/go-zookeeper/zk"
)

type ZKTester struct {
	conn    *zk.Conn
	servers []string
	done    chan struct{}
	wg      sync.WaitGroup
	logger  *log.Logger
}

// 初始化日志文件
func initLogger() (*log.Logger, *os.File, error) {
	// 创建logs目录
	if err := os.MkdirAll("logs", 0755); err != nil {
		return nil, nil, fmt.Errorf("创建日志目录失败: %v", err)
	}

	// 生成日志文件名（按日期）
	logFileName := filepath.Join("logs", fmt.Sprintf("zk_test_%s.log", time.Now().Format("2006-01-02")))

	// 打开日志文件（追加模式）
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("打开日志文件失败: %v", err)
	}

	// 创建新的logger
	logger := log.New(logFile, "", log.Ldate|log.Ltime|log.Lmicroseconds)

	return logger, logFile, nil
}

func NewZKTester(servers []string) (*ZKTester, error) {
	logger, _, err := initLogger()
	if err != nil {
		return nil, fmt.Errorf("初始化日志失败: %v", err)
	}

	return &ZKTester{
		servers: servers,
		done:    make(chan struct{}),
		logger:  logger,
	}, nil
}

func (z *ZKTester) Connect() error {
	// 设置连接事件回调
	eventCallback := func(event zk.Event) {
		z.logger.Printf("ZooKeeper事件: %+v\n", event)
	}

	// 建立连接
	conn, _, err := zk.Connect(z.servers, time.Second*5, zk.WithEventCallback(eventCallback))
	if err != nil {
		return fmt.Errorf("连接ZooKeeper失败: %v", err)
	}
	z.conn = conn
	return nil
}

func (z *ZKTester) RunStabilityTest() {
	z.wg.Add(2)

	// 启动连接状态监控
	go z.monitorConnection()
	// 启动基本操作测试
	go z.runBasicOperations()

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	close(z.done)
	z.wg.Wait()
	if z.conn != nil {
		z.conn.Close()
	}
}

func (z *ZKTester) monitorConnection() {
	defer z.wg.Done()
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		select {
		case <-z.done:
			return
		case <-ticker.C:
			if z.conn.State() != zk.StateHasSession {
				z.logger.Printf("警告: ZooKeeper连接状态异常: %v\n", z.conn.State())
			} else {
				z.logger.Printf("连接状态正常: %v\n", z.conn.State())
			}
		}
	}
}

func (z *ZKTester) runBasicOperations() {
	defer z.wg.Done()
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	testPath := "/zk_test"
	for {
		select {
		case <-z.done:
			return
		case <-ticker.C:
			// 创建测试节点
			exists, _, err := z.conn.Exists(testPath)
			if err != nil {
				z.logger.Printf("检查节点失败: %v\n", err)
				continue
			}

			if !exists {
				_, err = z.conn.Create(testPath, []byte("test_data"), 0, zk.WorldACL(zk.PermAll))
				if err != nil {
					z.logger.Printf("创建节点失败: %v\n", err)
					continue
				}
				z.logger.Printf("成功创建测试节点: %s\n", testPath)
			}

			// 读取节点数据
			data, _, err := z.conn.Get(testPath)
			if err != nil {
				z.logger.Printf("读取节点数据失败: %v\n", err)
				continue
			}
			z.logger.Printf("成功读取节点数据: %s\n", string(data))

			// 更新节点数据
			_, err = z.conn.Set(testPath, []byte(fmt.Sprintf("test_data_%d", time.Now().Unix())), -1)
			if err != nil {
				z.logger.Printf("更新节点数据失败: %v\n", err)
				continue
			}
			z.logger.Printf("成功更新节点数据\n")
		}
	}
}

func main() {
	// 配置ZooKeeper服务器地址
	servers := []string{"qa-zookeeper-1.x.com:2181", "qa-zookeeper-2.x.com:2181", "qa-zookeeper-3.x.com:2181"}

	tester, err := NewZKTester(servers)
	if err != nil {
		fmt.Printf("初始化测试器失败: %v\n", err)
		os.Exit(1)
	}

	err = tester.Connect()
	if err != nil {
		tester.logger.Fatalf("连接失败: %v", err)
	}

	tester.logger.Println("开始ZooKeeper连接稳定性测试...")
	tester.RunStabilityTest()
}
