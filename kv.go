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
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type KeyValueManager interface {
	// KeyValue will lookup and bind to an existing KeyValue store.
	KeyValue(bucket string) (KeyValue, error)
	// CreateKeyValue will create a KeyValue store with the following configuration.
	CreateKeyValue(cfg *KeyValueConfig) (KeyValue, error)
	// DeleteKeyValue will delete this KeyValue store (JetStream stream).
	DeleteKeyValue(bucket string) error
}

type KeyValue interface {
	// Get returns the latest value for the key.
	Get(key string) (entry KeyValueEntry, err error)
	// Put will place the new value for the key into the store.
	Put(key string, value []byte) (revision uint64, err error)
	// Create will add the key/value pair iff it does not exist.
	Create(key string, value []byte) (revision uint64, err error)
	// Update will update the value iff the latest revision matches.
	Update(key string, value []byte, last uint64) (revision uint64, err error)
	// Delete will place a delete marker and leave all revisions.
	Delete(key string) error
	// Purge will place a delete marker and remove all previous revisions.
	Purge(key string) error
	// PurgeDeletes will remove all current delete markers.
	PurgeDeletes() error
	// WatchAll will invoke the callback for all updates.
	WatchAll(cb KeyValueUpdate) (*Subscription, error)
	// Watch will invoke the callback for any keys that match keyPattern when they update.
	Watch(keys string, cb KeyValueUpdate) (*Subscription, error)
	// Keys() will return all keys
	Keys() (<-chan string, error)
	// History will return all historical values for the key.
	History(key string) ([]KeyValueEntry, error)
	// Bucket returns the current bucket name.
	Bucket() string
}

// KeyValueConfig is for configuring a KeyValue store.
type KeyValueConfig struct {
	Bucket       string
	Description  string
	MaxValueSize int32
	History      uint8
	TTL          time.Duration
	MaxBytes     int64
	Storage      StorageType
	Replicas     int
}

// Used to watch all keys.
const (
	KeyValueMaxHistory = 64
	AllKeys            = ">"
	kvop               = "KV-Operation"
	kvdel              = "DEL"
	kvpurge            = "PURGE"
)

type KeyValueOp uint8

const (
	KeyValuePut KeyValueOp = iota
	KeyValueDelete
	KeyValuePurge
)

func (op KeyValueOp) String() string {
	switch op {
	case KeyValuePut:
		return "KeyValuePutOp"
	case KeyValueDelete:
		return "KeyValueDeleteOp"
	case KeyValuePurge:
		return "KeyValuePurgeOp"
	default:
		return "Unknown Operation"
	}
}

// KeyValueEntry is a retrieved entry for Get or List or Watch.
type KeyValueEntry interface {
	// Bucket is the bucket the data was loaded from.
	Bucket() string
	// Key is the key that was retrieved.
	Key() string
	// Value is the retrieved value.
	Value() []byte
	// Revision is a unique sequence for this value.
	Revision() uint64
	// Created is the time the data was put in the bucket.
	Created() time.Time
	// Delta is distance from the latest value.
	Delta() uint64
	// Operation returns Put or Delete or Purge.
	Operation() KeyValueOp
}

// KeyValueUpdate is the callback handler for KeyValueEntry updates.
type KeyValueUpdate func(v KeyValueEntry)

// Errors
var (
	ErrKeyValueConfigRequired = errors.New("nats: config required")
	ErrInvalidBucketName      = errors.New("nats: invalid bucket name")
	ErrInvalidKey             = errors.New("nats: invalid key")
	ErrBucketNotFound         = errors.New("nats: bucket not found")
	ErrBadBucket              = errors.New("nats: bucket not valid key-value store")
	ErrKeyNotFound            = errors.New("nats: key not found")
	ErrKeyDeleted             = errors.New("nats: key was deleted")
	ErrHistoryToLarge         = errors.New("nats: history limited to a max of 64")
	ErrNoKeysFound            = errors.New("nats: no keys found")
)

const (
	kvBucketNameTmpl  = "KV_%s"
	kvSubjectsTmpl    = "$KV.%s.>"
	kvSubjectsPreTmpl = "$KV.%s."
	kvNoPending       = "0"
)

// Regex for valid keys and buckets.
var (
	validBucketRe = regexp.MustCompile(`\A[a-zA-Z0-9_-]+\z`)
	validKeyRe    = regexp.MustCompile(`\A[-/_=\.a-zA-Z0-9]+\z`)
)

// KeyValue will lookup and bind to an existing KeyValue store.
func (js *js) KeyValue(bucket string) (KeyValue, error) {
	if !js.nc.serverMinVersion(2, 6, 2) {
		return nil, errors.New("nats: key-value requires at least server version 2.6.2")
	}
	if !validBucketRe.MatchString(bucket) {
		return nil, ErrInvalidBucketName
	}
	stream := fmt.Sprintf(kvBucketNameTmpl, bucket)
	si, err := js.StreamInfo(stream)
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
		js:     js,
	}
	return kv, nil
}

// CreateKeyValue will create a KeyValue store with the following configuration.
func (js *js) CreateKeyValue(cfg *KeyValueConfig) (KeyValue, error) {
	if !js.nc.serverMinVersion(2, 6, 2) {
		return nil, errors.New("nats: key-value requires at least server version 2.6.2")
	}
	if cfg == nil {
		return nil, ErrKeyValueConfigRequired
	}
	if !validBucketRe.MatchString(cfg.Bucket) {
		return nil, ErrInvalidBucketName
	}
	if _, err := js.AccountInfo(); err != nil {
		return nil, err
	}

	// Default to 1 for history. Max is 64 for now.
	history := int64(1)
	if cfg.History > 0 {
		if cfg.History > KeyValueMaxHistory {
			return nil, ErrHistoryToLarge
		}
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
		MaxBytes:          cfg.MaxBytes,
		MaxAge:            cfg.TTL,
		MaxMsgSize:        cfg.MaxValueSize,
		Storage:           cfg.Storage,
		Replicas:          replicas,
		AllowRollup:       true,
		DenyDelete:        true,
	}

	if _, err := js.AddStream(scfg); err != nil {
		return nil, err
	}

	kv := &kvs{
		name:   cfg.Bucket,
		stream: scfg.Name,
		pre:    fmt.Sprintf(kvSubjectsPreTmpl, cfg.Bucket),
		js:     js,
	}
	return kv, nil
}

// DeleteKeyValue will delete this KeyValue store (JetStream stream).
func (js *js) DeleteKeyValue(bucket string) error {
	if !validBucketRe.MatchString(bucket) {
		return ErrInvalidBucketName
	}
	stream := fmt.Sprintf(kvBucketNameTmpl, bucket)
	return js.DeleteStream(stream)
}

type kvs struct {
	name   string
	stream string
	pre    string
	js     *js
}

// Underlying entry.
type kve struct {
	bucket   string
	key      string
	value    []byte
	revision uint64
	delta    uint64
	created  time.Time
	op       KeyValueOp
}

func (e *kve) Bucket() string        { return e.bucket }
func (e *kve) Key() string           { return e.key }
func (e *kve) Value() []byte         { return e.value }
func (e *kve) Revision() uint64      { return e.revision }
func (e *kve) Created() time.Time    { return e.created }
func (e *kve) Delta() uint64         { return e.delta }
func (e *kve) Operation() KeyValueOp { return e.op }

func keyValid(key string) bool {
	if len(key) == 0 || key[0] == '.' || key[len(key)-1] == '.' {
		return false
	}
	return validKeyRe.MatchString(key)
}

// Get returns the latest value for the key.
func (kv *kvs) Get(key string) (KeyValueEntry, error) {
	if !keyValid(key) {
		return nil, ErrInvalidKey
	}

	var b strings.Builder
	b.WriteString(kv.pre)
	b.WriteString(key)

	m, err := kv.js.GetLastMsg(kv.stream, b.String())
	if err != nil {
		if err == ErrMsgNotFound {
			err = ErrKeyNotFound
		}
		return nil, err
	}

	entry := &kve{
		bucket:   kv.name,
		key:      key,
		value:    m.Data,
		revision: m.Sequence,
		created:  m.Time,
	}

	// Double check here that this is not a DEL Operation marker.
	if len(m.Header) > 0 {
		switch m.Header.Get(kvop) {
		case kvdel:
			entry.op = KeyValueDelete
			return entry, ErrKeyDeleted
		case kvpurge:
			entry.op = KeyValuePurge
			return entry, ErrKeyDeleted
		}
	}

	return entry, nil
}

// Put will place the new value for the key into the store.
func (kv *kvs) Put(key string, value []byte) (revision uint64, err error) {
	if !keyValid(key) {
		return 0, ErrInvalidKey
	}

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
	v, err := kv.Update(key, value, 0)
	if err == nil {
		return v, nil
	}
	// TODO(dlc) - Since we have tombstones for DEL ops for watchers, this could be from that
	// so we need to double check.
	if e, err := kv.Get(key); err == ErrKeyDeleted {
		return kv.Update(key, value, e.Revision())
	}
	return 0, err
}

// Update will update the value iff the latest revision matches.
func (kv *kvs) Update(key string, value []byte, revision uint64) (uint64, error) {
	if !keyValid(key) {
		return 0, ErrInvalidKey
	}

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

// Delete will place a delete marker and leave all revisions.
func (kv *kvs) Delete(key string) error {
	return kv.delete(key, false)
}

// Purge will remove the key and all revisions.
func (kv *kvs) Purge(key string) error {
	return kv.delete(key, true)
}

func (kv *kvs) delete(key string, purge bool) error {
	if !keyValid(key) {
		return ErrInvalidKey
	}

	var b strings.Builder
	b.WriteString(kv.pre)
	b.WriteString(key)

	// DEL op marker. For watch functionality.
	m := NewMsg(b.String())

	if purge {
		m.Header.Set(kvop, kvpurge)
		m.Header.Set(MsgRollup, MsgRollupSubject)
	} else {
		m.Header.Set(kvop, kvdel)
	}
	_, err := kv.js.PublishMsg(m)
	return err
}

// PurgeDeletes will remove all current delete markers.
// This is a maintenance option if there is a larger buildup of delete markers.
func (kv *kvs) PurgeDeletes() error {
	o, cancel, err := getJSContextOpts(kv.js.opts)
	if err != nil {
		return err
	}
	if cancel != nil {
		defer cancel()
	}

	ctx := o.ctx
	if ctx == nil {
		ctx, cancel = context.WithTimeout(context.Background(), o.wait)
		defer cancel()
	}

	done := make(chan error, 1)
	sub, err := kv.WatchAll(func(v KeyValueEntry) {
		if v == nil {
			done <- nil
			return
		}
		var b strings.Builder
		b.WriteString(kv.pre)
		b.WriteString(v.Key())
		err := kv.js.purgeStream(kv.stream, &streamPurgeRequest{Subject: b.String()})
		if err != nil {
			done <- err
		}
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	// Wait on done or ctx/timeout.
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Keys() will return all keys.
func (kv *kvs) Keys() (<-chan string, error) {
	_, err := kv.js.GetLastMsg(kv.stream, AllKeys)
	if err != nil {
		if err == ErrMsgNotFound {
			err = ErrNoKeysFound
		}
		return nil, err
	}

	keys := make(chan string, 32)
	cb := func(m *Msg) {
		if len(m.Subject) <= len(kv.pre) {
			keys <- _EMPTY_
			m.Sub.Unsubscribe()
			return
		}
		subj := m.Subject[len(kv.pre):]
		keys <- subj

		tokens, err := getMetadataFields(m.Reply)
		if err != nil {
			keys <- _EMPTY_
			m.Sub.Unsubscribe()
		}
		pending := tokens[ackNumPendingTokenPos]
		if pending == kvNoPending {
			keys <- _EMPTY_
			m.Sub.Unsubscribe()
		}
	}

	var b strings.Builder
	b.WriteString(kv.pre)
	b.WriteString(AllKeys)

	_, err = kv.js.Subscribe(b.String(), cb, OrderedConsumer(), DeliverLastPerSubject(), HeadersOnly())
	if err != nil {
		return nil, err
	}

	return keys, nil
}

// History will return all values for the key.
func (kv *kvs) History(key string) ([]KeyValueEntry, error) {
	// Do quick check to make sure this is a legit key with history.
	if _, err := kv.Get(key); err != nil && err != ErrKeyDeleted {
		return nil, err
	}

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

	var vals []KeyValueEntry
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
			pending := tokens[ackNumPendingTokenPos]
			entry := &kve{
				bucket:   kv.name,
				key:      subj,
				value:    m.Data,
				revision: uint64(parseNum(tokens[ackStreamSeqTokenPos])),
				created:  time.Unix(0, parseNum(tokens[ackTimestampSeqTokenPos])),
				delta:    uint64(parseNum(pending)),
			}
			if len(m.Header) > 0 {
				switch m.Header.Get(kvop) {
				case kvdel:
					entry.op = KeyValueDelete
				case kvpurge:
					entry.op = KeyValuePurge
				}
			}
			vals = append(vals, entry)

			if pending == kvNoPending {
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
	var initDoneMarker bool

	// Could be a pattern so don't check for validity as we normally do.
	var b strings.Builder
	b.WriteString(kv.pre)
	b.WriteString(keys)
	keys = b.String()

	update := func(m *Msg) {
		tokens, err := getMetadataFields(m.Reply)
		if err != nil {
			return
		}
		if len(m.Subject) <= len(kv.pre) {
			return
		}
		subj := m.Subject[len(kv.pre):]

		var op KeyValueOp
		if len(m.Header) > 0 {
			switch m.Header.Get(kvop) {
			case kvdel:
				op = KeyValueDelete
			case kvpurge:
				op = KeyValuePurge
			}
		}
		delta := uint64(parseNum(tokens[ackNumPendingTokenPos]))
		cb(&kve{
			bucket:   kv.name,
			key:      subj,
			value:    m.Data,
			revision: uint64(parseNum(tokens[ackStreamSeqTokenPos])),
			created:  time.Unix(0, parseNum(tokens[ackTimestampSeqTokenPos])),
			delta:    delta,
			op:       op,
		})

		if !initDoneMarker && delta == 0 {
			initDoneMarker = true
			cb(nil)
		}
	}

	// Check if we have anything pending.
	_, err := kv.js.GetLastMsg(kv.stream, keys)
	if err == ErrMsgNotFound {
		initDoneMarker = true
		cb(nil)
	}

	// Used ordered consumer to deliver results.
	return kv.js.Subscribe(keys, update, OrderedConsumer(), DeliverLastPerSubject())
}

// Bucket returns the current bucket name (JetStream stream).
func (kv *kvs) Bucket() string {
	return kv.name
}
