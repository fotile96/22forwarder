package main

import (
	"flag"
	"io"
	"log"
	"net"
	"time"
)

var (
	localAddr  = flag.String("local", ":9999", "host:port to listen on")
	majorRemoteAddr = flag.String("major-remote", ":9200", "host:port preferred to forward to")
	minorRemoteAddr = flag.String("minor-remote", ":9200", "host:port to forward to as backup")
	prefix     = flag.String("p", "tcpforward: ", "String to prefix log output")
	pingInterval = flag.Int("ping-interval", 1000, "Interval to perform tcping RTT measurement")
	RTTSmoothWindowSize = flag.Int("rtt-window", 5, "Specify the number of latest samples used to calculate average RTT")
	majorUpThresh = flag.Int("up-thresh", 150, "The maximum RTT under which we regard the major remote as alive")
)

func sliceAvg(s []int) int {
	ret := 0
	for _, v := range(s) {
		ret += v
	}
	return ret / len(s)
}

func RTTworker(remote string, conn net.Conn, client net.Conn, closeOnUp bool) {
	// if remote is up, close both conn and client
	samples := make([]int, *RTTSmoothWindowSize)
	if closeOnUp {
		for i, _ := range (samples) {
			samples[i] = *pingInterval
		}
	}
	timer := time.NewTimer(0)
	for {
		measuredRTT := sliceAvg(samples)
		log.Printf("Measured RTT to %s: %d ms.", remote, measuredRTT)
		if (closeOnUp && measuredRTT <= *majorUpThresh) || (!closeOnUp && measuredRTT > *majorUpThresh) {
			defer conn.Close()
			defer client.Close()
			return
		}
		<- timer.C
		timer.Reset(time.Millisecond*time.Duration(*pingInterval))
		timeStart := time.Now()
		conn, err := net.DialTimeout("tcp", remote, time.Millisecond*time.Duration(*pingInterval))
		newSample := *pingInterval
		if err == nil {
			newSample = int(time.Since(timeStart).Milliseconds())
			conn.Close()
		}
		samples = append(samples[1:], newSample)
	}
}

func forward(conn net.Conn) {
	client, err := net.DialTimeout("tcp", *majorRemoteAddr, time.Millisecond*time.Duration(*pingInterval))
	majorIsUp := true
	if err != nil {
		log.Printf("Dial failed: %v", err)
		majorIsUp = false
		client, err = net.Dial("tcp", *minorRemoteAddr)
		if err != nil {
			log.Printf("Dial failed: %v", err)
			defer conn.Close()
			return
		}
	}
	log.Printf("Forwarding from %v to %v\n", conn.LocalAddr(), client.RemoteAddr())
	go func() {
		defer client.Close()
		defer conn.Close()
		io.Copy(client, conn)
	}()
	go func() {
		defer client.Close()
		defer conn.Close()
		io.Copy(conn, client)
	}()
	go RTTworker(*majorRemoteAddr, conn, client, !majorIsUp)
}

func main() {
	flag.Parse()
	log.SetPrefix(*prefix + ": ")

	listener, err := net.Listen("tcp", *localAddr)
	if err != nil {
		log.Fatalf("Failed to setup listener: %v", err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("ERROR: failed to accept listener: %v", err)
		}
		log.Printf("Accepted connection from %v\n", conn.RemoteAddr().String())
		go forward(conn)
	}
}
