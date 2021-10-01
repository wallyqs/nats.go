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
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type ObjectStore interface {
	// CreateObjectStore will create an object store.
	CreateObjectStore(cfg *ObjectConfig) error
	// SealObjectStore will seal the underlying stream.
	SealObjectStore(store string) error
	// DeleteObjectStore will delete the underlying stream for the named object.
	DeleteObjectStore(store string) error

	// PutObject will place the contents from the reader into a new object store.
	PutObject(store string, meta *ObjectMeta, reader io.Reader) error
	// GetObject will pull the object from the object store.
	GetObject(store, name string) (ObjectResult, error)
	// DeleteObject will delete the named object from a multi-use store.
	DeleteObject(store, name string) error

	// PutFile is convenience function to put a file into a new object store.
	PutFile(store string, filename string) error
	// GetFile is a convenience function to pull an object from a store and place it in a file.
	GetFile(store, name, outfile string) error
}

var (
	errBadMeta       = errors.New("nats: object stream meta information invalid")
	errBadObjectName = errors.New("nats: invalid object name")
)

// ObjectMeta is high level information about an object.
type ObjectMeta struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Headers     Header `json:"headers,omitempty"`
	ChunkSize   uint32 `json:"chunk_size,omitempty"`
}

// Config for the object store.
type ObjectConfig struct {
	Name        string        `json:"name"`
	Description string        `json:"description,omitempty"`
	Headers     Header        `json:"headers,omitempty"`
	TTL         time.Duration `json:"ttl,omitempty"`
	Storage     StorageType   `json:"storage"`
	Replicas    int           `json:"replicas"`
}

type ObjectInfo struct {
	ObjectMeta
	Size   uint64 `json:"size"`
	Chunks uint32 `json:"chunks"`
	Digest string `json:"digest,omitempty"`
}

// ObjectResult will return the underlying stream info and also be an io.ReadCloser.
type ObjectResult interface {
	io.ReadCloser
	Info() (*ObjectInfo, error)
	Error() error
}

const (
	objNameTmpl         = "OBJ_%s"
	objSubjectsPre      = "$O."
	objAllChunksPreTmpl = "$O.%s.DATA.>"
	objAllMetaPreTmpl   = "$O.%s.META.>"
	objChunksPreTmpl    = "$O.%s.DATA.%s"
	objMetaPreTmpl      = "$O.%s.META.%s"
	objNoPending        = "0"
	objDefaultChunkSize = 128 * 1024 // 128k
	objDigestType       = "sha-256="
	objDigestTmpl       = objDigestType + "%s"
	objChunkTokenHdr    = "Chunk-Token"
)

// CreateObjectStore will create an object store.
func (js *js) CreateObjectStore(cfg *ObjectConfig) error {
	if cfg == nil || cfg.Name == _EMPTY_ {
		return ErrStreamNameRequired
	}
	name := sanitizeName(cfg.Name)
	if !nameOk(name) {
		return ErrInvalidStreamName
	}
	if cfg.Replicas == 0 {
		cfg.Replicas = 1
	}

	chunks := fmt.Sprintf(objAllChunksPreTmpl, name)
	meta := fmt.Sprintf(objAllMetaPreTmpl, name)

	scfg := &StreamConfig{
		Name:        fmt.Sprintf(objNameTmpl, name),
		Description: cfg.Description,
		Subjects:    []string{chunks, meta},
		MaxAge:      cfg.TTL,
		Storage:     cfg.Storage,
		Replicas:    cfg.Replicas,
		Discard:     DiscardNew,
	}

	// Create our stream.
	_, err := js.AddStream(scfg)
	return err
}

func (js *js) SealObjectStore(store string) error {
	stream := fmt.Sprintf(objNameTmpl, store)
	si, err := js.StreamInfo(stream)
	if err != nil {
		return err
	}
	// Seal the stream from being able to take on more messages.
	cfg := si.Config
	cfg.Sealed = true
	_, err = js.UpdateStream(&cfg)
	return err
}

// DeleteObjectStore will delete the underlying stream for the named object.
func (js *js) DeleteObjectStore(store string) error {
	stream := fmt.Sprintf(objNameTmpl, store)
	return js.DeleteStream(stream)
}

func nameOk(name string) bool {
	if len(name) == 0 {
		return false
	} else if len(name) == 1 {
		if name == "*" || name == ">" {
			return false
		}
	}
	if strings.ContainsAny(name, " \t\r\n.") {
		return false
	}
	return true
}

func sanitizeName(name string) string {
	stream := strings.ReplaceAll(name, ".", "_")
	return strings.ReplaceAll(stream, " ", "_")
}

const (
	alpha         = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	chunkTokenLen = 8
)

func generateChunkToken() string {
	var b [chunkTokenLen]byte
	rn := rand.Uint64()
	for i, l := 0, rn; i < len(b); i++ {
		b[i] = alpha[l%base]
		l /= base
	}
	return string(b[:])
}

// PutObject will place the contents from the reader into a new stream.
func (ojs *js) PutObject(store string, meta *ObjectMeta, r io.Reader) error {
	if meta == nil {
		return errors.New("nats: object meta required")
	}
	name := sanitizeName(meta.Name)
	if !nameOk(name) {
		return errBadObjectName
	}

	// Create a random subject prefixed with the object stream name.

	chunkToken := generateChunkToken()
	chunkSubj := fmt.Sprintf(objChunksPreTmpl, store, chunkToken)
	metaSubj := fmt.Sprintf(objMetaPreTmpl, store, name)

	// For async error handling
	var perr error
	var mu sync.Mutex
	setErr := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		perr = err
	}
	getErr := func() error {
		mu.Lock()
		defer mu.Unlock()
		return perr
	}

	purgePartial := func() {
		stream := fmt.Sprintf(objNameTmpl, store)
		ojs.purgeStream(stream, &streamPurgeRequest{Subject: chunkSubj})
	}

	// Create our own JS context to handle errors etc.
	js, err := ojs.nc.JetStream(PublishAsyncErrHandler(func(js JetStream, _ *Msg, err error) { setErr(err) }))
	if err != nil {
		return err
	}

	if meta.ChunkSize == 0 {
		meta.ChunkSize = objDefaultChunkSize
	}

	m, h := NewMsg(chunkSubj), sha256.New()
	chunk, sent, total := make([]byte, meta.ChunkSize), 0, uint64(0)
	info := &ObjectInfo{ObjectMeta: *meta}

	for {
		n, err := r.Read(chunk)

		// EOF Processing.
		if err == io.EOF {
			// Finalize sha.
			sha := h.Sum(nil)
			// Place meta info.
			info.Size, info.Chunks = uint64(total), uint32(sent)
			info.Digest = fmt.Sprintf(objDigestTmpl, base64.URLEncoding.EncodeToString(sha[:]))
			mm := NewMsg(metaSubj)
			mm.Header.Set(objChunkTokenHdr, chunkToken)
			mm.Data, err = json.Marshal(info)
			if err != nil {
				purgePartial()
				return err
			}
			_, err = js.PublishMsgAsync(mm)
			if err != nil {
				purgePartial()
				return err
			}
			break
		} else if err != nil {
			purgePartial()
			return err
		}

		// Chunk processing.
		m.Data = chunk[:n]
		h.Write(m.Data)

		// Send msg itself.
		if _, err := js.PublishMsgAsync(m); err != nil {
			purgePartial()
			return err
		}
		if err := getErr(); err != nil {
			purgePartial()
			return err
		}
		// Update totals.
		sent++
		total += uint64(n)
	}

	// Wait for all to be processed.
	select {
	case <-js.PublishAsyncComplete():
		if err := getErr(); err != nil {
			return err
		}
	case <-time.After(ojs.opts.wait):
		return ErrTimeout
	}

	return nil
}

// ObjectResult impl.
type objResult struct {
	sync.Mutex
	info *ObjectInfo
	r    io.ReadCloser
	err  error
}

// GetObject will pull the object from the underlying stream.
func (js *js) GetObject(store, name string) (ObjectResult, error) {
	// Lookup the stream to get the bound subject.
	name = sanitizeName(name)
	if !nameOk(name) {
		return nil, errBadObjectName
	}

	// Grab last meta value we have.
	meta := fmt.Sprintf(objMetaPreTmpl, store, name)
	stream := fmt.Sprintf(objNameTmpl, store)

	m, err := js.GetLastMsg(stream, meta)
	if err != nil {
		return nil, err
	}

	chunkToken := m.Header.Get(objChunkTokenHdr)
	if chunkToken == _EMPTY_ {
		return nil, errBadMeta
	}
	var info ObjectInfo
	if err := json.Unmarshal(m.Data, &info); err != nil {
		return nil, errBadMeta
	}

	pr, pw := net.Pipe()
	result := &objResult{info: &info, r: pr}

	gotErr := func(m *Msg, err error) {
		pw.Close()
		m.Sub.Unsubscribe()
		result.setErr(err)
	}

	// For calculating sum256
	h := sha256.New()

	processChunk := func(m *Msg) {
		tokens, err := getMetadataFields(m.Reply)
		if err != nil {
			gotErr(m, err)
			return
		}

		// Write to our pipe.
		for b := m.Data; len(b) > 0; {
			n, err := pw.Write(b)
			if err != nil {
				gotErr(m, err)
				return
			}
			b = b[n:]
		}
		// Update sha256
		h.Write(m.Data)

		// Check if we are done.
		if tokens[ackNumPendingTokenPos] == objNoPending {
			pw.Close()
			m.Sub.Unsubscribe()

			// Make sure the digest matches.
			sha := h.Sum(nil)
			rsha, err := base64.URLEncoding.DecodeString(info.Digest)
			if err != nil {
				gotErr(m, err)
				return
			}
			if !bytes.Equal(sha[:], rsha) {
				gotErr(m, errors.New("nats: received corrupt object, digests do not match"))
				return
			}
		}
	}

	chunkSubj := fmt.Sprintf(objChunksPreTmpl, store, chunkToken)
	_, err = js.Subscribe(chunkSubj, processChunk, OrderedConsumer())
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (js *js) DeleteObject(store, name string) error {
	// Lookup the stream to get the bound subject.
	name = sanitizeName(name)
	if !nameOk(name) {
		return errBadObjectName
	}
	stream := fmt.Sprintf(objNameTmpl, store)

	// Grab last meta value we have.
	meta := fmt.Sprintf(objMetaPreTmpl, store, name)
	m, err := js.GetLastMsg(stream, meta)
	if err != nil {
		return err
	}
	chunkToken := m.Header.Get(objChunkTokenHdr)
	if chunkToken == _EMPTY_ {
		return errBadMeta
	}
	chunkSubj := fmt.Sprintf(objChunksPreTmpl, store, chunkToken)
	// Delete Meta
	err = js.DeleteMsg(stream, m.Sequence)
	if err != nil {
		return err
	}
	// Purge chunks for the object.
	return js.purgeStream(stream, &streamPurgeRequest{Subject: chunkSubj})
}

// PutFile is convenience function to put a file into an object store.
func (js *js) PutFile(store string, filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	return js.PutObject(store, &ObjectMeta{Name: filename}, f)
}

// GetFile is a convenience function to pull and object and place in a file.
func (js *js) GetFile(store, name, outfile string) error {
	// Expect file to be new.
	f, err := os.OpenFile(outfile, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	result, err := js.GetObject(store, name)
	if err != nil {
		defer os.Remove(f.Name())
		return err
	}
	// Stream copy to the file.
	_, err = io.Copy(f, result)
	return err
}

// Read impl.
func (o *objResult) Read(p []byte) (n int, err error) {
	o.Lock()
	defer o.Unlock()
	if o.err != nil {
		return 0, err
	}
	return o.r.Read(p)
}

// Close impl.
func (o *objResult) Close() error {
	return o.r.Close()
}

func (o *objResult) setErr(err error) {
	o.Lock()
	defer o.Unlock()
	o.err = err
}

func (o *objResult) Info() (*ObjectInfo, error) {
	o.Lock()
	defer o.Unlock()
	return o.info, o.err
}

func (o *objResult) Error() error {
	o.Lock()
	defer o.Unlock()
	return o.err
}
