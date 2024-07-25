package main

import (
	"log"
	"net"

	"github.com/livinlefevreloca/pgspanner/connection"
)

func main() {
	l, err := net.Listen("tcp", "localhost:8000")
	if err != nil {
		log.Fatal(err)
	}

	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go connection.ConnectionLoop(conn)
	}
}
