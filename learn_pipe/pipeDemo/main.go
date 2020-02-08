package main

import (
	"fmt"
	"learn/learn_pipe/pipeline"
	"log-agent/pkg/bufio"
	"os"
)

func main() {
	//const filename = "large.in"
	//const n = 80000000
	const filename = "small.in"
	const n = 64 // 8个字节组成组成一个uint64整型
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	out := pipeline.RandomSource(n)
	writer := bufio.NewWriter(file)
	pipeline.WriteSink(writer, out)
	writer.Flush() // buffer不写满，不会自动写入磁盘，需要flush一下

	file, err = os.Open(filename)
	if err != nil {
		panic(err)
	}
	in := pipeline.ReaderSource(bufio.NewReader(file), -1)

	count := 0
	for v := range in {
		fmt.Println(v)
		count++
		if count >= 512 {
			break
		}
	}
}

func MergeDemo() {
	p := pipeline.Merge(
		pipeline.InMemSort(
			pipeline.ArraySource(3, 2,
				1, 6, 7, 4)),
		pipeline.InMemSort(
			pipeline.ArraySource(13, 32,
				1, 5, 2, 11)))

	for v := range p {
		fmt.Println(v)
	}
}
