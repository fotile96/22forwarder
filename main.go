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
	RTTMeasureDestMajor = flag.String("rtt-dest-major", "", "A remote TCP endpoint used for RTT measurement when major is up")
	RTTMeasureDestMinor = flag.String("rtt-dest-minor", "", "A remote TCP endpoint used for RTT measurement when major is down")
	RTTSmoothWindowSize = flag.Int("rtt-window", 5, "Specify the number of latest samples used to calculate average RTT")
	majorUpThresh = flag.Int("up-thresh", 100, "The maximum RTT under which we think the major remote become alive")
	majorDownThresh = flag.Int("down-thresh", 120, "The minimum RTT under which we think the major remote become dead")
)

func sliceAvg(s []int) int {
	ret := 0
	for _, v := range(s) {
		ret += v
	}
	return ret / len(s)
}

var majorIsUp = true

func RTTworker(conn net.Conn, client net.Conn, done chan int) {
	var remote string
	samples := make([]int, *RTTSmoothWindowSize)
	if majorIsUp {
		remote = *RTTMeasureDestMajor
	} else {
		remote = *RTTMeasureDestMinor
		for i, _ := range (samples) {
			samples[i] = *pingInterval
		}
	}
	timer := time.NewTimer(0)
	time.Sleep(3 * time.Second)
	for {
		measuredRTT := sliceAvg(samples)
		log.Printf("Measured RTT to %s: %d ms.", remote, measuredRTT)
		if (!majorIsUp && measuredRTT <= *majorUpThresh) || (majorIsUp && measuredRTT > *majorDownThresh) {
			defer conn.Close()
			defer client.Close()
			majorIsUp = !majorIsUp
			return
		}
		<- timer.C
		select {
		case <- done:
			return
		default:
		}
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
	var client net.Conn
	var err error
	if majorIsUp {
		client, err = net.DialTimeout("tcp", *majorRemoteAddr, time.Millisecond*time.Duration(*pingInterval))
	} else {
		client, err = net.DialTimeout("tcp", *minorRemoteAddr, time.Millisecond*time.Duration(*pingInterval))
	}
	if err != nil {
		log.Printf("Dial failed: %v", err)
		majorIsUp = !majorIsUp
		defer conn.Close()
		return
	}
	log.Printf("Forwarding from %v to %v\n", conn.LocalAddr(), client.RemoteAddr())
	c := make(chan int, 2)
	go func() {
		defer client.Close()
		defer conn.Close()
		io.Copy(client, conn)
		c <- 0
	}()
	go func() {
		defer client.Close()
		defer conn.Close()
		io.Copy(conn, client)
		c <- 0
	}()
	// call RTTworker in this goroutine, hence allows at most one incoming connection
	RTTworker(conn, client, c)
}

func main() {
	flag.Parse()
	if *RTTMeasureDestMajor == "" {
		*RTTMeasureDestMajor = *majorRemoteAddr
	}
	if *RTTMeasureDestMinor == "" {
		*RTTMeasureDestMinor = *majorRemoteAddr
	}
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
		//no concurrent incoming connections
		forward(conn)
	}
}
