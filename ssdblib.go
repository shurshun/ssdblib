package ssdblib

import (
	"errors"
	"github.com/seefan/gossdb"
	"regexp"
	"strings"
	"time"
)

type SSDBBinLog struct {
	Capacity string
	MinSeq   string
	MaxSeq   string
}

type SSDBReplSlave struct {
	Id        string
	Type      string
	Status    string
	LastSeq   string
	CopyCount string
	SyncCount string
}

type SSDBInfo struct {
	Version     string
	Links       string
	TotalCalls  string
	DBSize      string
	BinLog      SSDBBinLog
	Replication map[string]SSDBReplSlave
	Clients 	int
}

type Client struct {
	gossdb.Client
}

func tryConnect(Host string, Port int, Timeout int) (*gossdb.Connectors, error) {

	timer := time.NewTimer(time.Duration(Timeout) * time.Second)
	defer timer.Stop()

	type Result struct {
		*gossdb.Connectors
		error
	}

	channel := make(chan *Result, 1)

	go func() {
		var err error
		var ssdbPool *gossdb.Connectors

		ssdbPool, err = gossdb.NewPool(&gossdb.Config{
			Host:             Host,
			Port:             Port,
			MinPoolSize:      1,
			MaxPoolSize:      1,
			AcquireIncrement: 1,
			GetClientTimeout: 1,
			MaxWaitSize:      1,
			MaxIdleTime:      1,
			HealthSecond:     1,
		})

		if err != nil {
			goto finished
		}
	finished:
		channel <- &Result{Connectors: ssdbPool, error: err}
	}()

	select {
	case <-timer.C:
		return nil, errors.New("Connection timeout!")
	case result := <-channel:
		if result.error != nil {
			return nil, result.error
		}
		return result.Connectors, nil
	}
}

func Init(Host string, Port int) (*Client, error) {

	pool, err := tryConnect(Host, Port, 2)

	if err != nil {
		return nil, err
	}

	gossdb.Encoding = true

	client, err := pool.NewClient()

	if err != nil {
		return nil, err
	}

	defer client.Close()

	cl := new(Client)
	cl.Client = *client

	return cl, nil
}

func (c *Client) Info() (*SSDBInfo, error) {
	resp, err := c.Do("info")
	
	if err != nil {
		return nil, err
	}

	status := new(SSDBInfo)

	if len(resp) > 1 && resp[0] == "ok" {
		status.Version = resp[3]
		status.Links = resp[5]
		status.TotalCalls = resp[7]
		status.DBSize = resp[9]
		status.Clients = 0

		info := strings.Join(resp[1:], "\n")

		r := "([A-Za-z_]+)[]?:[ ]+([A-Za-z0-9\\.\\-\\ :\"]+)"

		re := regexp.MustCompile(r)
		list := re.FindAllStringSubmatch(info, -1)

		BinLog := new(SSDBBinLog)

		BinLog.Capacity = list[0][2]
		BinLog.MinSeq = list[1][2]
		BinLog.MaxSeq = list[2][2]

		status.BinLog = *BinLog

		lastKey := ""
		ReplInfo := new(SSDBReplSlave)

		status.Replication = make(map[string]SSDBReplSlave)

		for _, v := range list {
			if v[1] == "client" {
				status.Clients++
			}
			if v[1] == "slaveof" {
				lastKey = strings.Split(v[2], ":")[0]
				ReplInfo = new(SSDBReplSlave)
			} else {
				if lastKey != "" {
					switch v[1] {
					case "id":
						ReplInfo.Id = v[2]
					case "type":
						ReplInfo.Type = v[2]
					case "status":
						ReplInfo.Status = v[2]
					case "last_seq":
						ReplInfo.LastSeq = v[2]
					case "copy_count":
						ReplInfo.CopyCount = v[2]
					case "sync_count":
						ReplInfo.SyncCount = v[2]
						status.Replication[lastKey] = *ReplInfo
						lastKey = ""
					}
				}
			}
		}
	}

	return status, nil
}
