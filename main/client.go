package main

import "github.com/manhdaovan/go-memdb/explorer"

func main() {
	sv := explorer.NewServer()
	sv.Run(":8888")
}