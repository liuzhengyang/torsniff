package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/marksamman/bencode"
	"github.com/spf13/cobra"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"encoding/json"
)

const (
	directory = "torrents"
)

func homeDir() string {
	env := "HOME"
	if runtime.GOOS == "windows" {
		env = "USERPROFILE"
	} else if runtime.GOOS == "plan9" {
		env = "home"
	}
	return os.Getenv(env)
}

type tfile struct {
	Path   string `json:"path"`
	Length int64 `json:"length"`
}

func (t *tfile) String() string {
	return fmt.Sprintf("name: %s\n, size: %d\n", t.Path, t.Length)
}

type torrent struct {
	InfoHash string   `json:"infohash"`
	Name     string   `json:"name"`
	Length   int64    `json:"length,omitempty"`
	Files    []*tfile `json:"files,omitempty"`
}

func (t *torrent) String() string {
	return fmt.Sprintf(
		"link: %s\nname: %s\nsize: %d\nfile: %d\n",
		fmt.Sprintf("magnet:?xt=urn:btih:%s", t.InfoHash),
		t.Name,
		t.Length,
		len(t.Files),
	)
}

func newTorrent(meta []byte, infohashHex string) (*torrent, error) {
	dict, err := bencode.Decode(bytes.NewBuffer(meta))
	if err != nil {
		return nil, err
	}
	t := &torrent{InfoHash: infohashHex}
	if name, ok := dict["name.utf-8"].(string); ok {
		t.Name = name
	} else if name, ok := dict["name"].(string); ok {
		t.Name = name
	}
	if length, ok := dict["length"].(int64); ok {
		t.Length = length
	}
	var totalSize int64
	var extractFiles = func(file map[string]interface{}) {
		var filename string
		var filelength int64
		if inter, ok := file["path.utf-8"].([]interface{}); ok {
			name := make([]string, len(inter))
			for i, v := range inter {
				name[i] = fmt.Sprint(v)
			}
			filename = strings.Join(name, "/")
		} else if inter, ok := file["path"].([]interface{}); ok {
			name := make([]string, len(inter))
			for i, v := range inter {
				name[i] = fmt.Sprint(v)
			}
			filename = strings.Join(name, "/")
		}
		if length, ok := file["length"].(int64); ok {
			filelength = length
			totalSize += filelength
		}
		t.Files = append(t.Files, &tfile{Path: filename, Length: filelength})
	}
	if files, ok := dict["files"].([]interface{}); ok {
		for _, file := range files {
			if f, ok := file.(map[string]interface{}); ok {
				extractFiles(f)
			}
		}
	}
	if t.Length == 0 {
		t.Length = totalSize
	}
	if len(t.Files) == 0 {
		t.Files = append(t.Files, &tfile{Path: t.Name, Length: t.Length})
	}
	return t, nil
}

type blacklist struct {
	container    sync.Map
	expiredAfter time.Duration
}

func newBlackList(expiredAfter time.Duration) *blacklist {
	b := &blacklist{expiredAfter: expiredAfter}
	go b.clean()
	return b
}

func (b *blacklist) in(addr net.Addr) bool {
	key := addr.String()
	v, ok := b.container.Load(key)
	if !ok {
		return false
	}
	c := v.(time.Time)
	if c.Sub(time.Now()) > b.expiredAfter {
		b.container.Delete(key)
		return false
	}
	return true
}

func (b *blacklist) add(addr net.Addr) {
	b.container.Store(addr.String(), time.Now())
}

func (b *blacklist) clean() {
	for range time.Tick(10 * time.Second) {
		now := time.Now()
		b.container.Range(func(k, v interface{}) bool {
			c := v.(time.Time)
			if c.Sub(now) > b.expiredAfter {
				b.container.Delete(k)
			}
			return true
		})
	}
}

type torsniff struct {
	laddr      string
	maxFriends int
	maxPeers   int
	secret     string
	timeout    time.Duration
	blacklist  *blacklist
	dir        string
}

func (t *torsniff) run() {
	tokens := make(chan struct{}, t.maxPeers)
	dht, err := newDHT(t.laddr)
	if err != nil {
		panic(err)
	}
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "144.34.135.168:9092"})
	if err != nil {
		panic(err.Error())
	}
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					log.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()
	log.Println("running, it may take a few minutes...")
	for ac := range dht.chAnnouncement {
		tokens <- struct{}{}
		go t.work(ac, tokens, p)
	}
}

func (t *torsniff) work(ac *announcement, tokens chan struct{}, producer *kafka.Producer) {
	defer func() {
		<-tokens
	}()
	if t.isTorrentExist(ac.infohashHex) {
		return
	}
	if t.blacklist.in(ac.peer) {
		return
	}
	wire := newMetaWire(string(ac.infohash), ac.peer.String())
	defer metaWirePool.Put(wire)
	data, err := wire.fetch()
	if err != nil {
		t.blacklist.add(ac.peer)
		return
	}
	t.saveTorrent(ac.infohashHex, data)
	torrent, err := newTorrent(data, ac.infohashHex)
	if err != nil {
		return
	}
	log.Println(torrent)
	sendLog(torrent, producer)
}

func sendLog(t *torrent, p *kafka.Producer) () {
	topic := "dht_infohash_torsniff"
	data, err := json.Marshal(t)
	if err != nil {
		panic(err.Error())
	}
	log.Printf("%s\n\n", data)
	p.Produce(&kafka.Message{
		TopicPartition:kafka.TopicPartition{Topic:&topic, Partition: kafka.PartitionAny},
		Value: data,
	}, nil)
	p.Flush(1000)
}

func (t *torsniff) isTorrentExist(infohashHex string) bool {
	name, _ := t.torrentPath(infohashHex)
	_, err := os.Stat(name)
	if os.IsNotExist(err) {
		return false
	}
	return err == nil
}

func (t *torsniff) saveTorrent(infohashHex string, data []byte) () {
	log.Printf("Save Path %s", infohashHex)
}

func (t *torsniff) torrentPath(infohashHex string) (name string, dir string) {
	dir = path.Join(t.dir, infohashHex[:2], infohashHex[len(infohashHex)-2:])
	name = path.Join(dir, infohashHex+".torrent")
	return
}

func main() {
	var addr string
	var port uint16
	var peers int
	var timeout time.Duration
	var dir string
	var verbose bool

	var maxFriends int
	var root = &cobra.Command{
		Use:          "torsniff",
		Short:        "torsniff - a sniffer fetching torrents from BT network",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			absDir, err := filepath.Abs(dir)
			if err != nil {
				panic(err)
			}
			log.SetFlags(0)
			if verbose {
				log.SetOutput(os.Stdout)
			} else {
				log.SetOutput(ioutil.Discard)
			}
			p := &torsniff{
				laddr:      fmt.Sprintf("%s:%d", addr, port),
				timeout:    timeout,
				maxFriends: maxFriends,
				maxPeers:   peers,
				secret:     randomPeerID(),
				dir:        absDir,
				blacklist:  newBlackList(10 * time.Minute),
			}
			p.run()
			return nil
		},
	}
	root.Flags().StringVarP(&addr, "addr", "a", "0.0.0.0", "listen on given address")
	root.Flags().Uint16VarP(&port, "port", "p", 6881, "listen on given port")
	root.Flags().IntVarP(&maxFriends, "maxFriends", "f", 500, "max fiends to make with per second")
	root.Flags().IntVarP(&peers, "peers", "e", 400, "max peers to connect to download torrents")
	root.Flags().DurationVarP(&timeout, "timeout", "t", 10*time.Second, "max time allowed for downloading torrents")
	root.Flags().StringVarP(&dir, "dir", "d", path.Join(homeDir(), directory), "the directory to store the torrents")
	root.Flags().BoolVarP(&verbose, "verbose", "v", true, "run in verbose mode")
	if err := root.Execute(); err != nil {
		panic(fmt.Errorf("could not start: %v", err))
	}
}
