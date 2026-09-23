/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package object

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	o2 "github.com/juicedata/juicefs/pkg/object/o2cloud"
)

func TestO2KeyScope(t *testing.T) {
	for _, key := range []string{"../other", "a/../other", "/absolute", "a//b", `a\b`} {
		if _, err := cleanO2Key(key); err == nil {
			t.Errorf("accepted %q", key)
		}
	}
}

func TestO2ListPagination(t *testing.T) {
	sessionFile := filepath.Join(t.TempDir(), "session.json")
	if err := os.WriteFile(sessionFile, []byte(`{"validationKey":"test"}`), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parent := r.URL.Query().Get("parentid")
		if r.URL.Path == "/sapi/media/folder" {
			folders := []map[string]any{}
			if parent == "10" {
				folders = append(folders, map[string]any{"id": 20, "name": "dir"})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"folders": folders}})
			return
		}
		if r.URL.Path == "/sapi/media" {
			files := []map[string]any{}
			if r.URL.Query().Get("folderid") == "10" {
				files = append(files, map[string]any{"id": 1, "name": "a", "size": 1}, map[string]any{"id": 2, "name": "b", "size": 2})
			}
			if r.URL.Query().Get("folderid") == "20" {
				files = append(files, map[string]any{"id": 3, "name": "c", "size": 3})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"media": files}})
			return
		}
		t.Errorf("unexpected path %s", r.URL.Path)
	}))
	defer server.Close()
	client, err := o2.New(o2.Config{APIURL: server.URL + "/sapi/", UploadURL: server.URL + "/upload", SessionFile: sessionFile, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	s := &o2Storage{client: client, rootName: "JUICEFS-O2-LAB", rootID: "10",
		recent: map[string]o2.Item{"new": {ID: "99", Name: "new", ParentID: "10", Size: 4, Mtime: time.Now()}}}
	var got []string
	token := ""
	for {
		items, more, next, err := s.List(context.Background(), "", "", token, "", 2, false)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range items {
			got = append(got, item.Key())
		}
		if !more {
			break
		}
		token = next
	}
	if want := []string{"a", "b", "dir/", "dir/c", "new"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if head, err := s.Head(context.Background(), "new"); err != nil || head.Size() != 4 {
		t.Fatalf("recent Head: %v %v", head, err)
	}
}

func TestO2PutUsesRecentID(t *testing.T) {
	sessionFile := filepath.Join(t.TempDir(), "session.json")
	if err := os.WriteFile(sessionFile, []byte(`{"validationKey":"test"}`), 0600); err != nil {
		t.Fatal(err)
	}
	var server *httptest.Server
	listCalls, uploads := 0, 0
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sapi/media":
			if r.URL.Query().Get("origin") != "" {
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"media": []any{map[string]any{"id": 42, "url": server.URL + "/download"}}}})
				return
			}
			listCalls++
			_, _ = io.WriteString(w, `{"data":{"media":[]}}`)
		case "/download":
			_, _ = io.WriteString(w, "data")
		case "/upload":
			uploads++
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client, err := o2.New(o2.Config{APIURL: server.URL + "/sapi/", UploadURL: server.URL + "/upload", SessionFile: sessionFile, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	s := &o2Storage{client: client, rootID: "10", recent: map[string]o2.Item{"block": {
		ID: "42", Name: "block", ParentID: "10", Size: 4, Mtime: time.Now(),
	}}}
	if err := s.Put(context.Background(), "block", strings.NewReader("data")); err != nil {
		t.Fatal(err)
	}
	if listCalls != 0 || uploads != 0 {
		t.Fatalf("repeated upload: %d listings and %d uploads", listCalls, uploads)
	}
}

func TestO2PutDoesNotResendUnconfirmedUpload(t *testing.T) {
	sessionFile := filepath.Join(t.TempDir(), "session.json")
	if err := os.WriteFile(sessionFile, []byte(`{"validationKey":"test"}`), 0600); err != nil {
		t.Fatal(err)
	}
	uploads := 0
	var visible atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sapi/media":
			if r.URL.Query().Get("origin") != "" {
				_, _ = io.WriteString(w, `{"data":{"media":[{"id":77,"url":"/download"}]}}`)
			} else if visible.Load() {
				_, _ = io.WriteString(w, `{"data":{"media":[{"id":77,"name":"block","size":4}]}}`)
			} else {
				_, _ = io.WriteString(w, `{"data":{"media":[]}}`)
			}
		case "/upload":
			uploads++
			_, _ = io.WriteString(w, `{"data":{}}`)
		case "/download":
			_, _ = io.WriteString(w, "data")
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client, err := o2.New(o2.Config{APIURL: server.URL + "/sapi/", UploadURL: server.URL + "/upload", SessionFile: sessionFile, HTTPClient: server.Client(), TPS: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	s := &o2Storage{client: client, rootID: "10"}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Put(ctx, "block", strings.NewReader("data")); err == nil {
		t.Fatal("unconfirmed upload unexpectedly succeeded")
	}
	if err := s.Put(context.Background(), "block", strings.NewReader("data")); err == nil || !strings.Contains(err.Error(), "refusing to resend") {
		t.Fatalf("second Put: %v", err)
	}
	if uploads != 1 {
		t.Fatalf("sent %d uploads for one key", uploads)
	}
	visible.Store(true)
	if err := s.Put(context.Background(), "block", strings.NewReader("data")); err != nil {
		t.Fatalf("reconcile visible upload: %v", err)
	}
	if uploads != 1 {
		t.Fatalf("sent %d uploads after reconciliation", uploads)
	}
}
