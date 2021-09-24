// Copyright 2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type KeyValue interface {
	// Get returns the latest value for the key.
	Get(key string) (value []byte, revision uint64, err error)
	// Put will place the new value for the key into the store.
	Put(key string, value []byte) (revision uint64, err error)
	// Create will add the key/value pair iff it does not exist.
	Create(key string, value []byte) (revision uint64, err error)
	// Update will update the value iff the latest revision matches.
	Update(key string, value []byte, last uint64) (revision uint64, err error)
	// Delete the key and all revisions.
	Delete(key string) error
	// WatchAll will invoke the callback for all updates.
	WatchAll(cb KeyValueUpdate) (*Subscription, error)
	// Watch will invoke the callback for any keys that match keyPattern when they update.
	Watch(keys string, cb KeyValueUpdate) (*Subscription, error)
	// List will return all values for the key.
	List(key string) ([]*KeyValueEntry, error)
	// Bucket returns the current bucket name (JetStream stream).
	Bucket() string
}

// KeyValueConfig is for configuring a KeyValue store.
type KeyValueConfig struct {
	Bucket       string
	Description  string
	History      uint8
	MaxMsgs      int64
	MaxBytes     int64
	MaxAge       time.Duration
	MaxValueSize int32
	Storage      StorageType
	Replicas     int
}

// Used to watch all keys.
const AllKeys = ">"

// Value is for listing all values with revisions.
type KeyValueEntry struct {
	Key      string
	Data     []byte
	Revision uint64
}

// Callback handler for KeyValue updates.
type KeyValueUpdate func(v *KeyValueEntry)

// Errors
var (
	ErrBucketNameRequired = errors.New("nats: bucket name is required")
	ErrInvalidBucketName  = errors.New("nats: invalid bucket name")
	ErrBucketNotFound     = errors.New("nats: bucket not found")
	ErrBadBucket          = errors.New("nats: bucket not valid key-value store")
)

const (
	kvBucketNameTmpl  = "KV_%s"
	kvSubjectsTmpl    = "$KV.%s.>"
	kvSubjectsPreTmpl = "$KV.%s."
	kvNoPending       = "0"
)

// KeyValue will lookup and bind to an existing KeyValue store.
func (nc *Conn) KeyValue(bucket string, opts ...JSOpt) (KeyValue, error) {
	if bucket == _EMPTY_ {
		return nil, ErrBucketNameRequired
	}
	if strings.Contains(bucket, ".") {
		return nil, ErrInvalidBucketName
	}
	jsc, err := nc.JetStream(opts...)
	if err != nil {
		return nil, err
	}
	stream := fmt.Sprintf(kvBucketNameTmpl, bucket)
	si, err := jsc.StreamInfo(stream)
	if err != nil {
		if err == ErrStreamNotFound {
			err = ErrBucketNotFound
		}
		return nil, err
	}
	// Do some quick sanity checks that this is a correctly formed stream for KV.
	// Max msgs per subject should be > 0.
	if si.Config.MaxMsgsPerSubject < 1 {
		return nil, ErrBadBucket
	}

	kv := &kvs{
		name:   bucket,
		stream: stream,
		pre:    fmt.Sprintf(kvSubjectsPreTmpl, bucket),
		nc:     nc,
		js:     jsc.(*js),
	}
	return kv, nil
}

// AddKeyValue will create a KeyValue store with the following configuration.
func (nc *Conn) AddKeyValue(cfg *KeyValueConfig, opts ...JSOpt) (KeyValue, error) {
	if cfg == nil || cfg.Bucket == _EMPTY_ {
		return nil, ErrBucketNameRequired
	}
	if strings.Contains(cfg.Bucket, ".") {
		return nil, ErrInvalidBucketName
	}

	jsc, err := nc.JetStream(opts...)
	if err != nil {
		return nil, err
	}
	if _, err = jsc.AccountInfo(); err != nil {
		return nil, err
	}

	// Default to 1 for history.
	history := int64(1)
	if cfg.History > 0 {
		history = int64(cfg.History)
	}
	replicas := cfg.Replicas
	if replicas == 0 {
		replicas = 1
	}

	scfg := &StreamConfig{
		Name:              fmt.Sprintf(kvBucketNameTmpl, cfg.Bucket),
		Description:       cfg.Description,
		Subjects:          []string{fmt.Sprintf(kvSubjectsTmpl, cfg.Bucket)},
		MaxMsgsPerSubject: history,
		MaxMsgs:           cfg.MaxMsgs,
		MaxBytes:          cfg.MaxBytes,
		MaxAge:            cfg.MaxAge,
		MaxMsgSize:        cfg.MaxValueSize,
		Storage:           cfg.Storage,
		Replicas:          replicas,
	}

	if _, err := jsc.AddStream(scfg); err != nil {
		return nil, err
	}

	kv := &kvs{
		name:   cfg.Bucket,
		stream: scfg.Name,
		pre:    fmt.Sprintf(kvSubjectsPreTmpl, cfg.Bucket),
		nc:     nc,
		js:     jsc.(*js),
	}
	return kv, nil
}

type kvs struct {
	name   string
	stream string
	pre    string
	nc     *Conn
	js     *js
}

// Get returns the latest value for the key.
func (kv *kvs) Get(key string) (value []byte, revision uint64, err error) {
	// Build by hand since simple and avoids stdlib JSON.
	var b strings.Builder
	b.WriteString("{\"last_by_subj\":\"")
	b.WriteString(kv.pre)
	b.WriteString(key)
	b.WriteString("\"}")

	o, cancel, err := getJSContextOpts(kv.js.opts)
	if err != nil {
		return nil, 0, err
	}
	if cancel != nil {
		defer cancel()
	}

	// Send request.
	subj := kv.js.apiSubj(fmt.Sprintf(apiMsgGetT, kv.stream))
	r, err := kv.nc.RequestWithContext(o.ctx, subj, []byte(b.String()))
	if err != nil {
		return nil, 0, err
	}

	// FIXME(dlc) - Be good to avoid stdlib JSON when possible.
	// Maybe: https://github.com/goccy/go-json
	var resp apiMsgGetResponse
	if err := json.Unmarshal(r.Data, &resp); err != nil {
		return nil, 0, err
	}
	if resp.Error != nil {
		return nil, 0, errors.New(resp.Error.Description)
	}

	return resp.Message.Data, resp.Message.Sequence, nil
}

// Put will place the new value for the key into the store.
func (kv *kvs) Put(key string, value []byte) (revision uint64, err error) {
	var b strings.Builder
	b.WriteString(kv.pre)
	b.WriteString(key)

	pa, err := kv.js.Publish(b.String(), value)
	if err != nil {
		return 0, err
	}
	return pa.Sequence, err
}

// Create will add the key/value pair iff it does not exist.
func (kv *kvs) Create(key string, value []byte) (revision uint64, err error) {
	return kv.Update(key, value, 0)
}

// Update will update the value iff the latest revision matches.
func (kv *kvs) Update(key string, value []byte, revision uint64) (uint64, error) {
	var b strings.Builder
	b.WriteString(kv.pre)
	b.WriteString(key)

	m := Msg{Subject: b.String(), Header: Header{}, Data: value}
	m.Header.Set(ExpectedLastSubjSeqHdr, strconv.FormatUint(revision, 10))

	pa, err := kv.js.PublishMsg(&m)
	if err != nil {
		return 0, err
	}
	return pa.Sequence, err
}

// Delete the key and all revisions.
func (kv *kvs) Delete(key string) error {
	o, cancel, err := getJSContextOpts(kv.js.opts)
	if err != nil {
		return err
	}
	if cancel != nil {
		defer cancel()
	}

	// Build by hand since simple and avoids stdlib JSON.
	var b strings.Builder
	b.WriteString("{\"filter\":\"")
	b.WriteString(kv.pre)
	b.WriteString(key)
	b.WriteString("\"}")

	// Send request.
	subj := kv.js.apiSubj(fmt.Sprintf(apiStreamPurgeT, kv.stream))
	r, err := kv.nc.RequestWithContext(o.ctx, subj, []byte(b.String()))
	if err != nil {
		return err
	}
	var resp streamPurgeResponse
	if err := json.Unmarshal(r.Data, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		return errors.New(resp.Error.Description)
	}
	return nil
}

// List will return all values for the key.
func (kv *kvs) List(key string) ([]*KeyValueEntry, error) {
	o, cancel, err := getJSContextOpts(kv.js.opts)
	if err != nil {
		return nil, err
	}
	if cancel != nil {
		defer cancel()
	}

	ctx := o.ctx
	if ctx == nil {
		ctx, cancel = context.WithTimeout(context.Background(), o.wait)
		defer cancel()
	}

	var vals []*KeyValueEntry
	done := make(chan error, 1)
	cb := func(m *Msg) {
		tokens, err := getMetadataFields(m.Reply)
		if err != nil {
			done <- err
		} else {
			if len(m.Subject) <= len(kv.pre) {
				done <- ErrBadSubject
				return
			}
			subj := m.Subject[len(kv.pre):]
			vals = append(vals, &KeyValueEntry{
				Key:      subj,
				Data:     m.Data,
				Revision: uint64(parseNum(tokens[ackStreamSeqTokenPos])),
			})
			if tokens[ackNumPendingTokenPos] == kvNoPending {
				done <- nil
			}
		}
	}

	var b strings.Builder
	b.WriteString(kv.pre)
	b.WriteString(key)

	// Used ordered consumer to deliver results.
	sub, err := kv.js.Subscribe(b.String(), cb, OrderedConsumer())
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()

	// Wait on done or ctx/timeout.
	select {
	case err := <-done:
		if err != nil {
			return nil, err
		}
		return vals, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// WatchAll watches all keys.
func (kv *kvs) WatchAll(cb KeyValueUpdate) (*Subscription, error) {
	return kv.Watch(AllKeys, cb)
}

// Watch will fire the callback when a key that matches the keys pattern is updated.
// keys needs to be a valid NATS subject.
func (kv *kvs) Watch(keys string, cb KeyValueUpdate) (*Subscription, error) {
	var b strings.Builder
	b.WriteString(kv.pre)
	b.WriteString(keys)

	update := func(m *Msg) {
		tokens, err := getMetadataFields(m.Reply)
		if err != nil {
			return
		}
		if len(m.Subject) <= len(kv.pre) {
			//done <- ErrBadSubject
			return
		}
		subj := m.Subject[len(kv.pre):]
		cb(&KeyValueEntry{
			Key:      subj,
			Data:     m.Data,
			Revision: uint64(parseNum(tokens[ackStreamSeqTokenPos])),
		})
	}
	// Used ordered consumer to deliver results.
	return kv.js.Subscribe(b.String(), update, OrderedConsumer(), DeliverLastPerSubject())
}

// Bucket returns the current bucket name (JetStream stream).
func (kv *kvs) Bucket() string {
	return kv.name
}
