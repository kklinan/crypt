package etcd3

import (
	"context"
	"errors"
	"fmt"

	"github.com/kklinan/crypt/backend"

	"go.etcd.io/etcd/clientv3"
)

// Client provides and manages an etcd v3 client session.
type Client struct {
	client    *clientv3.Client
	waitIndex uint64
}

// New creates a new client from a given configuration.
func New(machines []string) (*Client, error) {
	newClient, err := clientv3.New(clientv3.Config{
		Endpoints: machines,
	})

	if err != nil {
		return nil, fmt.Errorf("creating new etcd clientv3 for crypt.backend.Client: %v", err)
	}
	return &Client{client: newClient, waitIndex: 0}, nil
}

// Get retrieves keys.
func (c *Client) Get(key string) ([]byte, error) {
	getResp, err := c.client.Get(context.TODO(), key)
	if err != nil {
		return nil, err
	}
	return getResp.Kvs[0].Value, nil
}

// List return keys of this key prefix.
func (c *Client) List(key string) (backend.KVPairs, error) {
	getResp, err := c.client.Get(context.TODO(), key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var list backend.KVPairs
	for _, kv := range getResp.Kvs {
		list = append(list, &backend.KVPair{Key: string(kv.Key), Value: kv.Value})
	}
	return list, nil
}

// Set puts a key-value pair into etcd.
func (c *Client) Set(key string, value []byte) error {
	_, err := c.client.Put(context.TODO(), key, string(value))
	if err != nil {
		return err
	}
	return nil
}

// Watch watches on a key.
func (c *Client) Watch(key string, stop chan bool) <-chan *backend.Response {
	respChan := make(chan *backend.Response, 0)
	go func() {
		ctx, cancel := context.WithCancel(context.TODO())
		watchChan := c.client.Watch(ctx, key)
		go func() {
			<-stop
			cancel()
		}()

		for wchanResp := range watchChan {
			if wchanResp.Canceled {
				respChan <- &backend.Response{Value: nil, Error: errors.New("watcher is closed")}
			}
			c.waitIndex = uint64(wchanResp.Events[0].Kv.Version)
			respChan <- &backend.Response{Value: wchanResp.Events[0].Kv.Value, Error: nil}
		}
	}()
	return respChan
}
