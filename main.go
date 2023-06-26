package main

import (
	"bufio"
	"fmt"
	"io"
	"net"

	"github.com/go-sql-driver/mysql"
)

func main() {
	// 监听客户端连接
	listener, err := net.Listen("tcp", "localhost:3306")
	if err != nil {
		fmt.Println("Failed to listen:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Listening on localhost:3306...")

	for {
		// 接受客户端连接
		clientConn, err := listener.Accept()
		if err != nil {
			fmt.Println("Failed to accept client connection:", err)
			continue
		}

		// 连接到MySQL服务器
		mysqlConn, err := net.Dial("tcp", "mysql-server:3306")
		if err != nil {
			fmt.Println("Failed to connect to MySQL server:", err)
			continue
		}

		// 启动协程处理客户端到MySQL的数据转发
		go forwardData(clientConn, mysqlConn)

		// 启动协程处理MySQL到客户端的数据转发
		go forwardData(mysqlConn, clientConn)
	}
}

// 转发数据
func forwardData(src net.Conn, dest net.Conn) {
	reader := bufio.NewReader(src)
	writer := bufio.NewWriter(dest)

	for {
		// 读取源数据
		data, err := reader.Peek(5) // 读取前5个字节，用于解析MySQL协议
		if err != nil {
			fmt.Println("Failed to read data:", err)
			break
		}

		if isUpdateQuery(data) {
			// 如果是UPDATE语句，禁止执行，直接返回错误响应给客户端
			response := generateErrorResponse(1064, "You are not allowed to execute UPDATE queries")
			_, err = writer.Write(response)
			if err != nil {
				fmt.Println("Failed to write data:", err)
				break
			}

			err = writer.Flush()
			if err != nil {
				fmt.Println("Failed to flush writer:", err)
				break
			}
			break
		}

		// 将数据写入目标连接
		_, err = writer.Write(data)
		if err != nil {
			fmt.Println("Failed to write data:", err)
			break
		}

		// 刷新缓冲区，确保数据发送到目标连接
		err = writer.Flush()
		if err != nil {
			fmt.Println("Failed to flush writer:", err)
			break
		}

		// 读取剩余数据并转发
		_, err = io.CopyBuffer(dest, src, make([]byte, 4096))
		if err != nil {
			fmt.Println("Failed to forward data:", err)
			break
		}
	}

	src.Close()
	dest.Close()
}

// 检查查询是否为UPDATE语句
func isUpdateQuery(data []byte) bool {
	// 解析MySQL协议
	query, err := mysql.GetPacketLength(data)
	if err != nil {
		return false
	}

	// 检查查询类型
	switch query.(type) {
	case *mysql.ComQuery:
		return isUpdateStatement(query.(*mysql.ComQuery).Query)
	case *mysql.ComStmtPrepare:
		return isUpdateStatement(query.(*mysql.ComStmtPrepare).Query)
	default:
		return false
	}
}

// 检查查询语句是否为UPDATE语句
func isUpdateStatement(query string) bool {
	query = mysql.TrimNull(query)
	query = mysql.TrimComment(query)
	query = mysql.StripLeadingComments(query)

	// 检查查询语句是否以"UPDATE"开头
	return mysql.StartsWithUpdate(query)
}

// 生成错误响应
func generateErrorResponse(errorCode uint16, errorMessage string) []byte {
	errPacket := &mysql.SQLError{
		Number:  errorCode,
		Message: errorMessage,
	}

	// 生成MySQL错误响应数据包
	errPacketBytes, _ := errPacket.Pack()

	// 在错误响应数据包前面添加长度
	lengthEncodedErrPacket := mysql.AppendLengthEncodedInt(nil, uint64(len(errPacketBytes)))
	lengthEncodedErrPacket = append(lengthEncodedErrPacket, errPacketBytes...)

	// 添加错误标志位
	errorResponse := []byte{mysql.ErrPacket}
	errorResponse = append(errorResponse, lengthEncodedErrPacket...)

	return errorResponse
}
