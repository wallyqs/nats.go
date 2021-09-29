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
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nuid"
)

type ObjectStore interface {
	// PutObject will place the contents from the reader into a new stream.
	PutObject(cfg *ObjectConfig, reader io.Reader) error
	// GetObject will pull the object from the stream and write to the writer.
	GetObject(name string) (*ObjectResult, error)
	// PutFile is convenience function to put a file into an object store.
	PutFile(cfg *ObjectConfig, filename string) error
	// GetFile is a convenience function to pull and object and place in a file.
	GetFile(name, outfile string) error
	// DeleteObject will delete the underlying stream for the named object.
	DeleteObject(name string) error
}

// Config for the object.
type ObjectConfig struct {
	Name        string
	Description string
	Retain      time.Duration
	ChunkSize   int
	Storage     StorageType
	Replicas    int
}

// ObjectResult will return the underlying stream info and also be an io.ReadCloser.
type ObjectResult struct {
	*StreamInfo
	sync.Mutex
	err error
	r   io.ReadCloser
}

const (
	objNameTmpl         = "OBJ_%s"
	objSubjectsPre      = "$O."
	objSubjectsTmpl     = "$O.%s.>"
	objSubjectsPreTmpl  = "$O.%s."
	objNoPending        = "0"
	objDefaultChunkSize = 128 * 1024 // 128k
	objShaHdrOff        = 256
	objDigestType       = "sha-256="
	objDigestTmpl       = objDigestType + "%s"
)

// PutObject will place the contents from the reader into a new stream.
func (ojs *js) PutObject(cfg *ObjectConfig, r io.Reader) error {
	if cfg == nil || cfg.Name == _EMPTY_ {
		return ErrStreamNameRequired
	}
	if strings.Contains(cfg.Name, ".") {
		return ErrInvalidStreamName
	}
	if cfg.ChunkSize < 0 {
		return errors.New("nats: chunk size must be >= 0")
	}
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = objDefaultChunkSize
	}

	// Create a random subject prefixed with the object stream name.
	var sb strings.Builder
	sb.WriteString(objSubjectsPre)
	sb.WriteString(cfg.Name)
	sb.WriteString(".")
	sb.WriteString(nuid.Next()[:8])
	subj := sb.String()

	scfg := &StreamConfig{
		Name:        fmt.Sprintf(objNameTmpl, cfg.Name),
		Description: cfg.Description,
		Subjects:    []string{subj},
		MaxAge:      cfg.Retain,
		MaxMsgSize:  int32(cfg.ChunkSize + objShaHdrOff),
		Storage:     cfg.Storage,
		Replicas:    cfg.Replicas,
		Discard:     DiscardNew,
	}

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

	// Create our own JS context to handle errors etc.
	js, err := ojs.nc.JetStream(
		PublishAsyncErrHandler(func(js JetStream, _ *Msg, err error) { setErr(err) }),
	)
	if err != nil {
		return err
	}

	// Create our stream.
	if si, err := js.AddStream(scfg); err != nil {
		return err
	} else if si.State.Msgs != 0 || (si.State.FirstSeq != 0 && si.State.FirstSeq != 1) {
		return errors.New("nats: object stream must be empty")
	}

	m, h := NewMsg(subj), sha256.New()
	chunk, sent, total := make([]byte, cfg.ChunkSize), 0, uint64(0)

	for {
		n, err := r.Read(chunk)
		if err != nil {
			if err != io.EOF {
				js.DeleteStream(scfg.Name)
				return err
			}
			m.Data = nil // Signals EOF
			sha := h.Sum(nil)
			// Place sha256 and content-length in EOF msg.
			m.Header.Set("Digest", fmt.Sprintf(objDigestTmpl, base64.URLEncoding.EncodeToString(sha[:])))
			m.Header.Set("Content-Length", strconv.FormatUint(total, 10))
		} else {
			m.Data = chunk[:n]
			h.Write(m.Data)
		}
		// Send msg itself.
		if _, err := js.PublishMsgAsync(m); err != nil {
			js.DeleteStream(scfg.Name)
			return err
		}
		if err := getErr(); err != nil {
			js.DeleteStream(scfg.Name)
			return err
		}
		// Update totals.
		sent++
		total += uint64(n)

		// Check if we are done.
		if err != nil && err == io.EOF {
			break
		}
	}
	select {
	case <-js.PublishAsyncComplete():
		if err := getErr(); err != nil {
			return err
		}
	case <-time.After(ojs.opts.wait):
		return ErrTimeout
	}

	// Seal the stream from being able to take on more messages.
	scfg.MaxMsgs = int64(sent)
	scfg.MaxMsgSize = 1
	js.UpdateStream(scfg)

	return nil
}

// GetObject will pull the object from the underlying stream.
func (js *js) GetObject(name string) (*ObjectResult, error) {
	// Lookup the stream to get the bound subject.
	stream := fmt.Sprintf(objNameTmpl, name)
	si, err := js.StreamInfo(stream)
	if err != nil {
		return nil, err
	}
	if len(si.Config.Subjects) != 1 {
		return nil, errors.New("nats: stream required to have one subject")
	}

	// TODO(dlc) - Could get last msg here for correct Content-Length.

	pr, pw := net.Pipe()
	result := &ObjectResult{StreamInfo: si, r: pr}

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
		if tokens[ackNumPendingTokenPos] == objNoPending {
			pw.Close()
			m.Sub.Unsubscribe()

			// Check digest if we have one, this will be on the last msg.
			if sh := m.Header.Get("Digest"); strings.HasPrefix(sh, objDigestType) {
				rsha, err := base64.URLEncoding.DecodeString(sh[len(objDigestType):])
				if err != nil {
					gotErr(m, err)
					return
				}
				sha := h.Sum(nil)
				if !bytes.Equal(sha[:], rsha) {
					gotErr(m, errors.New("nats: received corrupt object, digests do not match"))
					return
				}
			}
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
	}

	_, err = js.Subscribe(si.Config.Subjects[0], processChunk, OrderedConsumer())
	if err != nil {
		return nil, err
	}

	return result, nil
}

// DeleteObject will delete the underlying stream.
func (js *js) DeleteObject(name string) error {
	// Lookup the stream to get the bound subject.
	stream := fmt.Sprintf(objNameTmpl, name)
	return js.DeleteStream(stream)
}

// PutFile is convenience function to put a file into an object store.
func (js *js) PutFile(cfg *ObjectConfig, file string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	return js.PutObject(cfg, f)
}

// GetFile is a convenience function to pull and object and place in a file.
func (js *js) GetFile(name, outfile string) error {
	// Expect file to be new.
	f, err := os.OpenFile(outfile, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	result, err := js.GetObject(name)
	if err != nil {
		defer os.Remove(f.Name())
		return err
	}
	// Stream copy to the file.
	_, err = io.Copy(f, result)
	return err
}

// Read impl.
func (o *ObjectResult) Read(p []byte) (n int, err error) {
	if err := o.error(); err != nil {
		return 0, err
	}
	return o.r.Read(p)
}

// Close impl.
func (o *ObjectResult) Close() error {
	return o.r.Close()
}

func (o *ObjectResult) setErr(err error) {
	o.Lock()
	defer o.Unlock()
	o.err = err
}

func (o *ObjectResult) error() error {
	o.Lock()
	defer o.Unlock()
	return o.err
}
