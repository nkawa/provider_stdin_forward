package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	sxutil "github.com/synerex/synerex_sxutil"
)

// datastore provider provides Datastore Service.

type DataStore interface {
	store(str string)
}

var (
	nodesrv   = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	channel   = flag.String("channel", "7", "forwarding channel type(default 7)")
	local     = flag.String("local", "", "Specify Local Synerex Server")
	verbose   = flag.Bool("verbose", false, "Verbose information")
	jst       = flag.Bool("jst", false, "Run/display with JST Time")
	speed     = flag.Float64("speed", 1.0, "Speed of sending packets(default real time =1.0), minus in msec")
	skip      = flag.Int("skip", 0, "Skip lines(default 0)")
	sxServerAddress string

)

func init() {

}

const dateFmt = "2006-01-02T15:04:05.999Z"

func atoUint(s string) uint32 {
	r, err := strconv.Atoi(s)
	if err != nil {
		log.Print("err", err)
	}
	return uint32(r)
}

// sending stdin File.
func sendingStdIn(client *sxutil.SXServiceClient) {
	// file


	scanner := bufio.NewScanner(os.Stdin) // csv reader
	var buf []byte = make([]byte, 1024)
	scanner.Buffer(buf, 1024*1024*64) // 64Mbytes buffer

//	last := time.Now()
//	started := false // start flag
	skipCount := 0

//	jstZone := time.FixedZone("Asia/Tokyo", 9*60*60)

	for scanner.Scan() { // read one line.
		if *skip != 0 { // if there is skip  , do it first
			skipCount++
			if skipCount < *skip {
				continue
			}
			log.Printf("Skip %d:", *skip)
			skipCount = 0
		}

		dt := scanner.Text()
			if *verbose {
				log.Printf("Scan:%s", dt)
			}

		{ // sending each packets
			
			smo := sxutil.SupplyOpts{
				Name:  "stdin",
				JSON:  dt,
				Cdata: nil,
			}
			_, nerr := client.NotifySupply(&smo)
			if nerr != nil { // connection failuer with current client
				// we need to ask to nodeidserv?
				// or just reconnect.
				newClient := sxutil.GrpcConnectServer(sxServerAddress)
				if newClient != nil {
					log.Printf("Reconnect Server %s\n", sxServerAddress)
					client.SXClient = newClient
				}
			} else {
				log.Printf("chan:%s,len:%d",  *channel,len(dt))
			}
			if *speed < 0 { // sleep for each packet
				time.Sleep(time.Duration(-*speed) * time.Millisecond)
			}

		}

	}

	serr := scanner.Err()
	if serr != nil {
		log.Printf("Scanner error %v", serr)
	}

}

//dataServer(pc_client)

func main() {
	log.Printf("StdIn Forward Provider(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	// check channel types.
	//
	channelTypes := []uint32{}
	chans := strings.Split(*channel, ",")
	for _, ch := range chans {
		v, err := strconv.Atoi(ch)
		if err == nil {
			channelTypes = append(channelTypes, uint32(v))
		} else {
			log.Fatal("Can't convert channels ", *channel)
		}
	}

	srv, rerr := sxutil.RegisterNode(*nodesrv, fmt.Sprintf("StdInForward[%s]", *channel), channelTypes, nil)

	sxServerAddress =srv


	if rerr != nil {
		log.Fatal("Can't register node:", rerr)
	}
	if *local != "" { // quick hack for AWS local network
		sxServerAddress = *local
	}
	log.Printf("Connecting SynerexServer at [%s]", sxServerAddress)

	//	wg := sync.WaitGroup{} // for syncing other goroutines

	client := sxutil.GrpcConnectServer(sxServerAddress)

	if client == nil {
		log.Fatal("Can't connect Synerex Server")
	} else {
		log.Print("Connecting SynerexServer")
	}

	argJson := fmt.Sprintf("{StdInForward[%d]}", channelTypes[0])
	sclient := sxutil.NewSXServiceClient(client, channelTypes[0], argJson)

	sendingStdIn(sclient)

}
