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

package o2cloud

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func mockClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	c, err := New(Config{APIURL: server.URL + "/sapi/", UploadURL: server.URL + "/upload", User: "unused", Password: "unused", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	c.session = Session{ValidationKey: "test-key"}
	return c
}

func TestO2UploadConfirmsReturnedID(t *testing.T) {
	uploads, lists := 0, 0
	c := mockClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sapi/media":
			lists++
			if lists == 1 {
				_, _ = io.WriteString(w, `{"data":{"media":[]}}`)
				return
			}
			_, _ = io.WriteString(w, `{"data":{"media":[{"id":41,"name":"block","size":4},{"id":42,"name":"block","size":4}]}}`)
		case "/upload":
			uploads++
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Error(err)
			}
			file, _, err := r.FormFile("file")
			if err != nil {
				t.Error(err)
				return
			}
			body, _ := io.ReadAll(file)
			file.Close()
			if string(body) != "data" {
				t.Errorf("uploaded %q", body)
			}
			if r.FormValue("data") == "" {
				t.Error("missing O2 metadata")
			}
			_, _ = io.WriteString(w, `{"data":{"media":{"id":42}}}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	})
	item, err := c.Upload(context.Background(), "10", "block", strings.NewReader("data"), 4)
	if err != nil {
		t.Fatal(err)
	}
	if item.ID != "42" || uploads != 1 || lists != 2 {
		t.Fatalf("got ID %s after %d uploads and %d listings", item.ID, uploads, lists)
	}
}

func TestO2UploadResponsePrefersMediaID(t *testing.T) {
	payload := map[string]any{"data": map[string]any{
		"folder": map[string]any{"id": float64(10)},
		"media":  map[string]any{"file": map[string]any{"id": float64(42), "name": "block"}},
	}}
	if id := uploadResponseID(payload, "block"); id != "42" {
		t.Fatalf("got ID %s", id)
	}
}

func TestO2AmbiguousUploadIsNotRepeated(t *testing.T) {
	requests := 0
	c := mockClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/upload" {
			requests++
			w.WriteHeader(504)
			return
		}
		if r.URL.Path == "/sapi/media" {
			if requests == 0 {
				_, _ = io.WriteString(w, `{"data":{"media":[]}}`)
			} else {
				_, _ = io.WriteString(w, `{"data":{"media":[{"id":99,"name":"block","size":4}]}}`)
			}
			return
		}
		t.Errorf("unexpected path %s", r.URL.Path)
	})
	item, err := c.Upload(context.Background(), "10", "block", strings.NewReader("data"), 4)
	if err != nil {
		t.Fatal(err)
	}
	if requests != 1 || item.ID != "99" {
		t.Fatalf("uploads=%d ID=%s", requests, item.ID)
	}
}

func TestO2FilesUsesAPIPages(t *testing.T) {
	pages := 0
	c := mockClient(t, func(w http.ResponseWriter, r *http.Request) {
		pages++
		if r.URL.Query().Get("offset") != strconv.Itoa((pages-1)*pageSize) {
			t.Errorf("offset %s", r.URL.Query().Get("offset"))
		}
		count := pageSize
		if pages == 2 {
			count = 1
		}
		list := make([]map[string]any, count)
		for i := range list {
			list[i] = map[string]any{"id": pages*1000 + i, "name": strconv.Itoa(pages*1000 + i), "size": 4}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"media": list}})
	})
	items, err := c.Files(context.Background(), "10")
	if err != nil {
		t.Fatal(err)
	}
	if pages != 2 || len(items) != 201 {
		t.Fatalf("pages=%d items=%d", pages, len(items))
	}
	shortPages := 0
	shortClient := mockClient(t, func(w http.ResponseWriter, r *http.Request) {
		shortPages++
		if r.URL.Query().Get("offset") != strconv.Itoa((shortPages-1)*pageSize) {
			t.Errorf("short page offset %s", r.URL.Query().Get("offset"))
		}
		if shortPages == 1 {
			_, _ = io.WriteString(w, `{"data":{"media":[{"id":1,"name":"first","size":4}],"more":true}}`)
		} else {
			_, _ = io.WriteString(w, `{"data":{"media":[{"id":2,"name":"second","size":4}],"more":false}}`)
		}
	})
	items, err = shortClient.Files(context.Background(), "10")
	if err != nil {
		t.Fatal(err)
	}
	if shortPages != 2 || len(items) != 2 {
		t.Fatalf("short pages=%d items=%d", shortPages, len(items))
	}
}

func TestO2DownloadRange(t *testing.T) {
	c := mockClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/media" {
			_, _ = io.WriteString(w, `{"data":{"media":[{"url":"/data"}]}}`)
			return
		}
		if got := r.Header.Get("Range"); got != "bytes=2-4" {
			t.Errorf("range=%s", got)
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, "cde")
	})
	r, err := c.Download(context.Background(), Item{ID: "1", Size: 10}, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "cde" {
		t.Fatalf("got %q", b)
	}
}

func TestO2DownloadWaitsForMediaID(t *testing.T) {
	resolves := 0
	c := mockClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/media" {
			resolves++
			if resolves == 1 {
				_, _ = io.WriteString(w, `{"data":{"media":[]}}`)
				return
			}
			_, _ = io.WriteString(w, `{"data":{"media":[{"url":"/data"}]}}`)
			return
		}
		_, _ = io.WriteString(w, "ready")
	})
	r, err := c.Download(context.Background(), Item{ID: "42"}, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	b, _ := io.ReadAll(r)
	if resolves != 2 || string(b) != "ready" {
		t.Fatalf("resolves=%d data=%q", resolves, b)
	}
}

func TestO2RenewsSessionAfterUnauthorizedRead(t *testing.T) {
	renewals := 0
	c := mockClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/login/oauth" {
			renewals++
			_, _ = io.WriteString(w, `{"data":{"validationkey":"new-key"}}`)
			return
		}
		if r.URL.Query().Get("validationkey") == "test-key" {
			w.WriteHeader(401)
			return
		}
		_, _ = io.WriteString(w, `{"data":{"folders":[{"id":20,"name":"dir"}]}}`)
	})
	c.session.OAuthBundle = `{ "access_token": "placeholder" }`
	items, err := c.Folders(context.Background(), "10")
	if err != nil {
		t.Fatal(err)
	}
	if renewals != 1 || len(items) != 1 || items[0].ID != "20" {
		t.Fatalf("renewals=%d items=%v", renewals, items)
	}
}

func TestO2DeleteRetriesUnvalidatedID(t *testing.T) {
	deletes := 0
	c := mockClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/media/file" {
			deletes++
			if deletes == 1 {
				_, _ = io.WriteString(w, `{"error":{"code":"MED-1017"}}`)
				return
			}
			_, _ = io.WriteString(w, `{"data":{"success":true}}`)
			return
		}
		if r.URL.Path == "/sapi/media" {
			_, _ = io.WriteString(w, `{"data":{"media":[]}}`)
			return
		}
		t.Errorf("unexpected path %s", r.URL.Path)
	})
	if err := c.Delete(context.Background(), Item{ID: "42", Name: "block", ParentID: "10", Kind: "file"}); err != nil {
		t.Fatal(err)
	}
	if deletes != 2 {
		t.Fatalf("deletes=%d", deletes)
	}
}
