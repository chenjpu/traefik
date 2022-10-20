// Package nomad contains the nomad store implementation.
package nomadVars

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/hashicorp/nomad/api"
	"github.com/kvtools/valkeyrie"
	"github.com/kvtools/valkeyrie/store"
)

// StoreName the name of the store.
const StoreName = "nomad"

const (
	// DefaultWatchWaitTime is how long we block for at a time to check if the watched key has changed.
	// This affects the minimum time it takes to cancel a watch.
	DefaultWatchWaitTime = 15 * time.Second

	// RenewSessionRetryMax is the number of time we should try to renew the session before giving up and throwing an error.
	RenewSessionRetryMax = 5

	// MaxSessionDestroyAttempts is the maximum times we will try
	// to explicitly destroy the session attached to a lock after
	// the connectivity to the store has been lost.
	MaxSessionDestroyAttempts = 5

	// defaultLockTTL is the default ttl for the consul lock.
	defaultLockTTL = 20 * time.Second
)

var (
	// ErrMultipleEndpointsUnsupported is thrown when there are multiple endpoints specified for Consul.
	ErrMultipleEndpointsUnsupported = errors.New("nomad does not support multiple endpoints")
)

// registers Consul to Valkeyrie.
func init() {
	valkeyrie.Register(StoreName, newStore)
}

// Config the Consul configuration.
type Config struct {
	TLS               *tls.Config
	ConnectionTimeout time.Duration
	Token             string
	Namespace         string
}

func newStore(ctx context.Context, endpoints []string, options valkeyrie.Config) (store.Store, error) {
	cfg, ok := options.(*Config)
	if !ok && options != nil {
		return nil, &store.InvalidConfigurationError{Store: StoreName, Config: options}
	}

	return New(ctx, endpoints, cfg)
}

// Store implements the store.Store interface.
type Store struct {
	client *api.Client
}

// New creates a new Consul client.
func New(_ context.Context, endpoints []string, options *Config) (*Store, error) {
	if len(endpoints) > 1 {
		return nil, ErrMultipleEndpointsUnsupported
	}

	config := createConfig(endpoints, options)

	// Creates a new client.
	client, err := api.NewClient(config)
	if err != nil {
		return nil, err
	}

	return &Store{client: client}, nil
}

// Get the value at "key".
// Returns the last modified index to use in conjunction to CAS calls.
func (s *Store) Get(_ context.Context, key string, opts *store.ReadOptions) (*store.KVPair, error) {
	options := &api.QueryOptions{
		AllowStale: false,
	}
	pair, meta, err := s.client.Variables().Read(normalize(key), options)
	if err != nil {
		if err.Error() == api.ErrVariableNotFound {
			return nil, store.ErrKeyNotFound
		} else {
			return nil, err
		}
	}
	value, err := json.Marshal(pair.Items)
	if err != nil {
		return nil, err
	}
	return &store.KVPair{Key: pair.Path, Value: value, LastIndex: meta.LastIndex}, nil
}

// Put a value at "key".
func (s *Store) Put(_ context.Context, key string, value []byte, opts *store.WriteOptions) error {
	panic("implement me")
}

// Delete a value at "key".
func (s *Store) Delete(ctx context.Context, key string) error {
	panic("implement me")
}

// Exists checks that the key exists inside the store.
func (s *Store) Exists(ctx context.Context, key string, opts *store.ReadOptions) (bool, error) {
	_, err := s.Get(ctx, key, opts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// List child nodes of a given directory.
func (s *Store) List(_ context.Context, directory string, opts *store.ReadOptions) ([]*store.KVPair, error) {
	options := &api.QueryOptions{
		AllowStale: false,
	}

	if opts != nil && !opts.Consistent {
		options.AllowStale = true
	}

	var varApi = s.client.Variables()

	varMetas, _, err := varApi.PrefixList(normalize(directory), options)
	if err != nil {
		return nil, err
	}
	if len(varMetas) == 0 {
		return nil, store.ErrKeyNotFound
	}
	var kv []*store.KVPair
	for _, vn := range varMetas {
		if vars, _, err := varApi.Read(vn.Path, options); err == nil {
			for k, v := range vars.Items {
				if v == "~" {
					v = ""
				}
				kv = append(kv, &store.KVPair{
					Key:       vn.Path + "/" + strings.ReplaceAll(k, ".", "/"),
					Value:     []byte(v),
					LastIndex: vars.ModifyIndex,
				})
			}
		}
	}
	return kv, nil
}

// DeleteTree deletes a range of keys under a given directory.
func (s *Store) DeleteTree(ctx context.Context, directory string) error {
	panic("implement me")
}

// Watch for changes on a "key".
// It returns a channel that will receive changes or pass on errors.
// Upon creation, the current value will first be sent to the channel.
// Providing a non-nil stopCh can be used to stop watching.
func (s *Store) Watch(ctx context.Context, key string, _ *store.ReadOptions) (<-chan *store.KVPair, error) {
	kv := s.client.Variables()
	watchCh := make(chan *store.KVPair)

	go func() {
		defer close(watchCh)

		// Use a wait time in order to check if we should quit from time to time.
		opts := &api.QueryOptions{WaitTime: DefaultWatchWaitTime}

		for {
			// Check if we should quit.
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Get the key.
			pair, meta, err := kv.Read(key, opts)
			if err != nil {
				return
			}

			// If LastIndex didn't change then it means `Get` returned
			// because of the WaitTime and the key didn't change.
			if opts.WaitIndex == meta.LastIndex {
				continue
			}
			opts.WaitIndex = meta.LastIndex

			// Return the value to the channel.
			if pair != nil {
				value, err := json.Marshal(pair.Items)
				if err != nil {
					return
				}
				watchCh <- &store.KVPair{
					Key:       pair.Path,
					Value:     value,
					LastIndex: pair.ModifyIndex,
				}
			}
		}
	}()

	return watchCh, nil
}

// WatchTree watches for changes on a "directory".
// It returns a channel that will receive changes or pass on errors.
// Upon creating a watch, the current children values will be sent to the channel.
// Providing a non-nil stopCh can be used to stop watching.
func (s *Store) WatchTree(ctx context.Context, directory string, _ *store.ReadOptions) (<-chan []*store.KVPair, error) {
	kv := s.client.Variables()
	watchCh := make(chan []*store.KVPair)

	go func() {
		defer close(watchCh)

		// Use a wait time in order to check if we should quit from time to time.
		opts := &api.QueryOptions{WaitTime: DefaultWatchWaitTime}
		for {
			// Check if we should quit.
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Get all the children.
			varMetas, meta, err := kv.PrefixList(directory, opts)
			if err != nil {
				return
			}

			// If LastIndex didn't change then it means `Get` returned
			// because of the WaitTime and the child keys didn't change.
			if opts.WaitIndex == meta.LastIndex {
				continue
			}
			opts.WaitIndex = meta.LastIndex

			// Return children KV pairs to the channel.
			var kvPairs []*store.KVPair

			for _, vn := range varMetas {
				if vars, _, err := kv.Read(vn.Path, opts); err == nil {
					for k, v := range vars.Items {
						if v == "~" {
							v = ""
						}
						kvPairs = append(kvPairs, &store.KVPair{
							Key:       vn.Path + "/" + strings.ReplaceAll(k, ".", "/"),
							Value:     []byte(v),
							LastIndex: vars.ModifyIndex,
						})
					}
				}
			}
			watchCh <- kvPairs
		}
	}()

	return watchCh, nil
}

// NewLock returns a handle to a lock struct which can be used to provide mutual exclusion on a key.
func (s *Store) NewLock(ctx context.Context, key string, opts *store.LockOptions) (store.Locker, error) {
	panic("implement me")
}

// AtomicPut puts a value at "key" if the key has not been modified in the meantime,
// throws an error if this is the case.
func (s *Store) AtomicPut(ctx context.Context, key string, value []byte, previous *store.KVPair, _ *store.WriteOptions) (bool, *store.KVPair, error) {
	panic("implement me")
}

// AtomicDelete deletes a value at "key" if the key has not been modified in the meantime,
// throws an error if this is the case.
func (s *Store) AtomicDelete(ctx context.Context, key string, previous *store.KVPair) (bool, error) {
	panic("implement me")
}

// Close closes the client connection.
func (s *Store) Close() error { return nil }

func createConfig(endpoints []string, options *Config) *api.Config {
	config := api.DefaultConfig()
	config.HttpClient = http.DefaultClient
	config.Address = endpoints[0]

	if options != nil {
		if options.TLS != nil {
			config.HttpClient.Transport = &http.Transport{
				TLSClientConfig: options.TLS,
			}
		}

		if options.ConnectionTimeout != 0 {
			config.WaitTime = options.ConnectionTimeout
		}

		if options.Token != "" {
			config.SecretID = options.Token
		}

		if options.Namespace != "" {
			config.Namespace = options.Namespace
		}
	}

	return config
}

// normalize the key for usage in Consul.
func normalize(key string) string {
	return strings.TrimPrefix(key, "/")
}
