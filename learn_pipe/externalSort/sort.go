package main

import (
	"fmt"
	"learn/learn_pipe/pipeline"
	"log-agent/pkg/bufio"
	"os"
	"strconv"
)

func main() {
	p := CreatePipeline("small.in", 512,
		4)
	//p := CreateNetWorkPipeline("small.in", 512, 4)
	//p := CreateNetWorkPipeline("large.in", 512000000, 4)
	//time.Sleep(time.Hour)
	writeToFile(p, "small.out")
	printFile("small.out")
}

func printFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	p := pipeline.ReaderSource(file, -1)
	count := 0
	for v := range p {
		fmt.Println(v)
		count++
		if count >= 512 {
			break
		}
	}
}
func writeToFile(p <-chan int, filename string) {

	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	defer writer.Flush()
	pipeline.WriteSink(writer, p)
}

func CreatePipeline(filename string, fileSize, chunCount int) <-chan int {
	pipeline.Init()
	chunSize := fileSize / chunCount

	sourceResult := []<-chan int{}

	for i := 0; i < chunCount; i++ {
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}
		file.Seek(int64(i*chunSize), 0)
		source := pipeline.ReaderSource(bufio.NewReader(file), chunSize)
		// sort
		sourceResult = append(sourceResult, pipeline.InMemSort(source))
	}

	return pipeline.MergeN(sourceResult...)
}

func CreateNetPipeline(filename string, fileSize, chunCount int) <-chan int {
	pipeline.Init()
	chunSize := fileSize / chunCount

	//sourceResult := []<-chan int{}
	sortAddr := []string{}

	// 服务端程序，在不同的服务器上进行数据的整合
	for i := 0; i < chunCount; i++ {
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}
		file.Seek(int64(i*chunSize), 0)
		source := pipeline.ReaderSource(bufio.NewReader(file), chunSize)

		addr := ":" + strconv.Itoa(7000+i)
		// sort  server
		pipeline.NetworkSink(addr, pipeline.InMemSort(source))
		sortAddr = append(sortAddr, addr)
		//sourceResult = append(sourceResult, pipeline.InMemSort(source))
	}

	// 客户端，将排序好的数据从服务器上拉取下来
	sourceResult := []<-chan int{}
	for _, addr := range sortAddr {
		//client 去连接上上面的server,读取数据
		sourceResult = append(sourceResult,
			pipeline.NetWorkSource(addr))
	}

	// 拉取下来的数据进行整合（可以将这个操作也分下去）
	return pipeline.MergeN(sourceResult...)
}
