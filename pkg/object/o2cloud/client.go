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

// Package o2cloud contains the O2 Cloud protocol client used by JuiceFS.
package o2cloud

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultAPIURL    = "https://cloud.o2online.es/sapi/"
	DefaultUploadURL = "https://upload.cloud.o2online.es/sapi/upload"
	pageSize         = 200
)

type Cookie struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Session struct {
	ValidationKey   string   `json:"validationKey"`
	Cookies         []Cookie `json:"cookies"`
	UserAgent       string   `json:"userAgent"`
	OAuthBundle     string   `json:"oauthBundle"`
	DeviceID        string   `json:"deviceId"`
	DeviceName      string   `json:"deviceName"`
	EncryptionToken string   `json:"encryptionToken"`
}

type Item struct {
	ID       string
	Name     string
	ParentID string
	Folder   bool
	Size     int64
	Mtime    time.Time
	Kind     string
}

type Config struct {
	APIURL, UploadURL, SessionFile, User, Password string
	TPS                                            float64
	Retries                                        int
	HTTPClient                                     *http.Client
}

type Client struct {
	apiURL, uploadURL, sessionFile, user, password string
	http                                           *http.Client
	mu                                             sync.Mutex
	session                                        Session
	sessionStamp                                   time.Time
	tick                                           <-chan time.Time
	stop                                           func()
	retries                                        int
}

func New(cfg Config) (*Client, error) {
	if cfg.APIURL == "" {
		cfg.APIURL = DefaultAPIURL
	}
	if cfg.UploadURL == "" {
		cfg.UploadURL = DefaultUploadURL
	}
	if _, err := url.ParseRequestURI(cfg.APIURL); err != nil {
		return nil, err
	}
	if _, err := url.ParseRequestURI(cfg.UploadURL); err != nil {
		return nil, err
	}
	if cfg.SessionFile == "" && (cfg.User == "" || cfg.Password == "") {
		return nil, errors.New("O2CLOUD_SESSION_FILE or both access and secret keys are required")
	}
	if cfg.TPS < 0 {
		return nil, errors.New("O2CLOUD_TPS cannot be negative")
	}
	if cfg.Retries < 0 {
		return nil, errors.New("O2CLOUD_RETRIES cannot be negative")
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 5 * time.Minute, Transport: http.DefaultTransport.(*http.Transport).Clone()}
	}
	c := &Client{apiURL: strings.TrimRight(cfg.APIURL, "/") + "/", uploadURL: cfg.UploadURL,
		sessionFile: cfg.SessionFile, user: cfg.User, password: cfg.Password, http: cfg.HTTPClient, retries: cfg.Retries}
	if c.retries == 0 {
		c.retries = 3
	}
	if cfg.TPS > 0 {
		period := time.Duration(float64(time.Second) / cfg.TPS)
		if period < time.Millisecond {
			period = time.Millisecond
		}
		ticker := time.NewTicker(period)
		c.tick, c.stop = ticker.C, ticker.Stop
	}
	if cfg.SessionFile != "" {
		if err := c.reloadSession(); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}

func (c *Client) Close() {
	if c.stop != nil {
		c.stop()
	}
}

func (c *Client) throttle(ctx context.Context) error {
	if c.tick == nil {
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.tick:
		return nil
	}
}

func (c *Client) reloadSession() error {
	info, err := os.Stat(c.sessionFile)
	if err != nil {
		return err
	}
	if info.ModTime().Equal(c.sessionStamp) && c.session.ValidationKey != "" {
		return nil
	}
	data, err := os.ReadFile(c.sessionFile)
	if err != nil {
		return err
	}
	var session Session
	if err = json.Unmarshal(data, &session); err != nil {
		return fmt.Errorf("invalid O2 session file: %w", err)
	}
	if session.ValidationKey == "" {
		return errors.New("O2 session file lacks validationKey")
	}
	c.session, c.sessionStamp = session, info.ModTime()
	return nil
}

func (c *Client) getSession(ctx context.Context) (Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessionFile != "" {
		if err := c.reloadSession(); err != nil {
			return Session{}, err
		}
	}
	if c.session.ValidationKey == "" {
		if err := c.login(ctx); err != nil {
			return Session{}, err
		}
	}
	return c.session, nil
}

func (c *Client) renew(ctx context.Context, expired Session) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session.ValidationKey != expired.ValidationKey {
		return nil
	}
	if c.sessionFile != "" {
		if err := c.reloadSession(); err != nil {
			return err
		}
		if c.session.ValidationKey != expired.ValidationKey {
			return nil
		}
	}
	if expired.OAuthBundle == "" {
		if c.sessionFile != "" {
			return errors.New("O2 session expired; refresh the session file")
		}
		return c.login(ctx)
	}
	if err := c.throttle(ctx); err != nil {
		return err
	}
	u := c.apiURL + "login/oauth?action=login&responsetime=true"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(""))
	if err != nil {
		return err
	}
	headers(expired, req)
	c.originHeaders(req)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	req.Header.Set("Accept", "*/*")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return statusError{resp.StatusCode}
	}
	var payload map[string]any
	if err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil {
		return err
	}
	if err = apiError(payload); err != nil {
		return err
	}
	data := asMap(payload["data"])
	key := stringField(data, "validationkey", "validationKey")
	if key == "" {
		return errors.New("O2 session renewal returned no validation key")
	}
	c.session.ValidationKey = key
	if auth := resp.Header.Get("Authorization"); auth != "" {
		c.session.OAuthBundle = auth
	}
	for _, cookie := range resp.Cookies() {
		found := false
		for i := range c.session.Cookies {
			if c.session.Cookies[i].Name == cookie.Name {
				c.session.Cookies[i].Value = cookie.Value
				found = true
				break
			}
		}
		if !found {
			c.session.Cookies = append(c.session.Cookies, Cookie{Name: cookie.Name, Value: cookie.Value})
		}
	}
	return nil
}

func (c *Client) login(ctx context.Context) error {
	form := url.Values{"login": {c.user}, "password": {c.password}}
	u := c.apiURL + "login?action=login"
	if err := c.throttle(ctx); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("O2 login HTTP %d", resp.StatusCode)
	}
	var payload map[string]any
	if err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil {
		return err
	}
	if err = apiError(payload); err != nil {
		return err
	}
	key := stringField(asMap(payload["data"]), "validationkey", "validationKey")
	if key == "" {
		return errors.New("O2 login returned no validation key")
	}
	c.session = Session{ValidationKey: key, UserAgent: "JuiceFS/o2cloud", DeviceID: "juicefs", DeviceName: "juicefs"}
	return nil
}

func randomID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return base64.RawURLEncoding.EncodeToString(b[:])
}

func headers(session Session, req *http.Request) {
	agent := session.UserAgent
	if agent == "" {
		agent = "JuiceFS/o2cloud"
	}
	req.Header.Set("User-Agent", agent)
	req.Header.Set("X-deviceid", session.DeviceID)
	req.Header.Set("X-request-id", randomID())
	req.Header.Set("Accept-Language", "es-ES,en,*")
	if session.DeviceName != "" {
		req.Header.Set("X-devicename", session.DeviceName)
	}
	if session.OAuthBundle != "" {
		bundle := strings.TrimPrefix(session.OAuthBundle, "oauth ")
		if _, err := base64.StdEncoding.DecodeString(bundle); err != nil || json.Valid([]byte(bundle)) {
			bundle = base64.StdEncoding.EncodeToString([]byte(bundle))
		}
		req.Header.Set("Authorization", "oauth "+bundle)
	}
	var cookies []string
	for _, cookie := range session.Cookies {
		if cookie.Name != "" && cookie.Value != "" {
			cookies = append(cookies, cookie.Name+"="+cookie.Value)
		}
	}
	if len(cookies) > 0 {
		req.Header.Set("Cookie", strings.Join(cookies, "; "))
	}
}

func (c *Client) originHeaders(req *http.Request) {
	origin := strings.TrimSuffix(strings.Split(c.apiURL, "/sapi")[0], "/")
	req.Header.Set("Origin", origin)
	req.Header.Set("Referer", origin+"/")
}

type statusError struct{ code int }

func (e statusError) Error() string { return fmt.Sprintf("O2 Cloud HTTP %d", e.code) }
func retryable(err error) bool {
	var status statusError
	if errors.As(err, &status) {
		return status.code == 408 || status.code == 429 || status.code >= 500
	}
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return !strings.HasPrefix(err.Error(), "O2 API error:")
}

func (c *Client) request(ctx context.Context, method, resource string, params url.Values, body []byte) (map[string]any, error) {
	if params == nil {
		params = url.Values{}
	}
	readOnly := method == http.MethodGet || params.Get("action") == "get" || params.Get("action") == "list"
	maxRetries := 0
	if readOnly {
		maxRetries = c.retries
	}
	for attempt := 0; attempt <= maxRetries; attempt++ {
		session, err := c.getSession(ctx)
		if err != nil {
			return nil, err
		}
		q := url.Values{}
		for k, v := range params {
			q[k] = append([]string(nil), v...)
		}
		q.Set("validationkey", session.ValidationKey)
		u := c.apiURL + strings.TrimLeft(resource, "/") + "?" + q.Encode()
		if err = c.throttle(ctx); err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, method, u, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		headers(session, req)
		c.originHeaders(req)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := c.http.Do(req)
		if err == nil {
			if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
				resp.Body.Close()
				if readOnly && attempt == 0 {
					if renewErr := c.renew(ctx, session); renewErr != nil {
						return nil, renewErr
					}
					continue
				}
				return nil, statusError{resp.StatusCode}
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				err = statusError{resp.StatusCode}
				resp.Body.Close()
			} else {
				var payload map[string]any
				err = json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&payload)
				resp.Body.Close()
				if errors.Is(err, io.EOF) {
					err = nil
				}
				if err == nil {
					err = apiError(payload)
				}
				if err == nil {
					return payload, nil
				}
			}
		}
		if !retryable(err) || attempt == maxRetries {
			return nil, err
		}
		if err = pause(ctx, time.Duration(1<<min(attempt, 4))*time.Second); err != nil {
			return nil, err
		}
	}
	return nil, errors.New("O2 request exhausted retries")
}

func pause(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func marshal(value any) []byte       { data, _ := json.Marshal(value); return data }
func asMap(value any) map[string]any { result, _ := value.(map[string]any); return result }
func asArray(value any) []any        { result, _ := value.([]any); return result }
func stringField(m map[string]any, names ...string) string {
	for _, n := range names {
		if v, ok := m[n].(string); ok && v != "" {
			return v
		}
		if v, ok := m[n].(json.Number); ok {
			return v.String()
		}
		if v, ok := m[n].(float64); ok {
			return strconv.FormatInt(int64(v), 10)
		}
	}
	return ""
}
func intField(m map[string]any, names ...string) int64 {
	for _, n := range names {
		if v, ok := m[n].(float64); ok {
			return int64(v)
		}
		if v, ok := m[n].(string); ok {
			x, _ := strconv.ParseInt(v, 10, 64)
			return x
		}
	}
	return 0
}
func nested(m map[string]any) map[string]any {
	for _, n := range []string{"media", "file", "item", "metadata", "data"} {
		if v := asMap(m[n]); v != nil {
			return v
		}
	}
	return m
}
func mediaString(m map[string]any, names ...string) string {
	if v := stringField(m, names...); v != "" {
		return v
	}
	return stringField(nested(m), names...)
}
func mediaInt(m map[string]any, names ...string) int64 {
	if v := intField(m, names...); v != 0 {
		return v
	}
	return intField(nested(m), names...)
}
func apiError(payload map[string]any) error {
	if payload == nil {
		return nil
	}
	v := payload["error"]
	if v == nil || v == false || v == "0" || v == "OK" || v == "SUCCESS" || v == "COM-0000" || v == float64(0) {
		return nil
	}
	return fmt.Errorf("O2 API error: %v", v)
}
func idValue(id string) any {
	if n, err := strconv.ParseInt(id, 10, 64); err == nil {
		return n
	}
	return id
}

func (c *Client) Root(ctx context.Context) (Item, error) {
	p, err := c.request(ctx, http.MethodPost, "media/folder/root", url.Values{"action": {"get"}}, nil)
	if err != nil {
		return Item{}, err
	}
	folder := asMap(p["rootfolder"])
	if data := asMap(p["data"]); data != nil {
		if list := asArray(data["folders"]); len(list) > 0 {
			folder = asMap(list[0])
		}
	}
	if folder == nil {
		folder = p
	}
	id := stringField(folder, "id", "folderid", "folderId", "uuid")
	if id == "" {
		return Item{}, errors.New("O2 root response lacks folder ID")
	}
	return Item{ID: id, Name: stringField(folder, "name"), Folder: true}, nil
}

func (c *Client) Folders(ctx context.Context, parent string) ([]Item, error) {
	p, err := c.request(ctx, http.MethodGet, "media/folder", url.Values{"action": {"list"}, "parentid": {parent}}, nil)
	if err != nil {
		return nil, err
	}
	list := asArray(asMap(p["data"])["folders"])
	if list == nil {
		list = asArray(p["folders"])
	}
	var items []Item
	for _, raw := range list {
		m := asMap(raw)
		id := stringField(m, "id", "folderid", "folderId", "uuid")
		if id == parent {
			id = stringField(m, "folderid", "folderId", "uuid")
		}
		if id != "" && id != parent {
			items = append(items, Item{ID: id, Name: stringField(m, "name"), ParentID: parent, Folder: true})
		}
	}
	return items, nil
}

var mediaFields = []string{"name", "modificationdate", "creationdate", "size", "origin", "folderid", "uploaded"}

func (c *Client) Files(ctx context.Context, parent string) ([]Item, error) {
	var items []Item
	seen := map[string]bool{}
	for offset := 0; ; offset += pageSize {
		params := url.Values{"action": {"get"}, "folderid": {parent}, "limit": {strconv.Itoa(pageSize)}, "offset": {strconv.Itoa(offset)}}
		p, err := c.request(ctx, http.MethodPost, "media", params, marshal(map[string]any{"data": map[string]any{"fields": mediaFields}}))
		if err != nil {
			return nil, err
		}
		data := asMap(p["data"])
		var list []any
		for _, name := range []string{"media", "files", "videos", "audios", "pictures", "images", "items"} {
			if list = asArray(data[name]); len(list) > 0 {
				break
			}
		}
		if len(list) == 0 {
			for _, name := range []string{"media", "files", "items"} {
				if list = asArray(p[name]); len(list) > 0 {
					break
				}
			}
		}
		added := 0
		for _, raw := range list {
			m := asMap(raw)
			name := mediaString(m, "name", "filename", "title")
			id := mediaString(m, "id", "mediaid", "mediaId", "fdoid", "uuid")
			if name == "" || id == "" || seen[id] {
				continue
			}
			seen[id] = true
			items = append(items, Item{ID: id, Name: name, ParentID: parent, Size: mediaInt(m, "size", "filesize", "fileSize"),
				Mtime: parseTime(mediaString(m, "modificationdate", "creationdate", "uploaded")), Kind: kind(name, mediaString(m, "type", "mimetype"))})
			added++
		}
		if len(list) == 0 || added == 0 || (!boolField(data, "more") && len(list) < pageSize) {
			break
		}
		if offset > 100000 {
			return nil, errors.New("O2 listing exceeded page limit")
		}
	}
	return items, nil
}

func boolField(m map[string]any, name string) bool {
	switch v := m[name].(type) {
	case bool:
		return v
	case string:
		value, _ := strconv.ParseBool(v)
		return value
	}
	return false
}

func parseTime(raw string) time.Time {
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if n > 10_000_000_000 {
			return time.UnixMilli(n)
		}
		return time.Unix(n, 0)
	}
	t, _ := time.Parse(time.RFC3339, raw)
	return t
}
func kind(name, contentType string) string {
	s := strings.ToLower(contentType)
	if strings.Contains(s, "video") {
		return "video"
	}
	if strings.Contains(s, "audio") {
		return "audio"
	}
	if strings.Contains(s, "image") {
		return "picture"
	}
	switch strings.ToLower(path.Ext(name)) {
	case ".mp4", ".mkv", ".avi":
		return "video"
	case ".mp3", ".m4a", ".flac":
		return "audio"
	case ".jpg", ".jpeg", ".png":
		return "picture"
	}
	return "file"
}

func (c *Client) Find(ctx context.Context, parent, name string, folder bool) (Item, error) {
	var items []Item
	var err error
	if folder {
		items, err = c.Folders(ctx, parent)
	} else {
		items, err = c.Files(ctx, parent)
	}
	if err != nil {
		return Item{}, err
	}
	var found Item
	for _, item := range items {
		if strings.EqualFold(item.Name, name) {
			if found.ID != "" {
				return Item{}, fmt.Errorf("multiple O2 objects match %q", name)
			}
			found = item
		}
	}
	if found.ID == "" {
		return Item{}, os.ErrNotExist
	}
	return found, nil
}

func (c *Client) EnsureFolder(ctx context.Context, parent, name string) (Item, error) {
	if item, err := c.Find(ctx, parent, name, true); err == nil {
		return item, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return Item{}, err
	}
	p, err := c.request(ctx, http.MethodPost, "media/folder", url.Values{"action": {"save"}}, marshal(map[string]any{"data": map[string]any{"name": name, "parentid": idValue(parent)}}))
	if err == nil {
		m := asMap(p["data"])
		if child := asMap(m["folder"]); child != nil {
			m = child
		}
		id := stringField(m, "id", "folderid", "folderId", "uuid")
		if id != "" && id != parent {
			return Item{ID: id, Name: name, ParentID: parent, Folder: true}, nil
		}
	}
	for i := 0; i < 12; i++ {
		item, lookupErr := c.Find(ctx, parent, name, true)
		if lookupErr == nil {
			return item, nil
		}
		if err2 := pause(ctx, time.Second); err2 != nil {
			return Item{}, err2
		}
	}
	if err != nil {
		return Item{}, err
	}
	return Item{}, fmt.Errorf("O2 did not confirm folder %q", name)
}

func (c *Client) Download(ctx context.Context, item Item, off, limit int64) (io.ReadCloser, error) {
	var u string
	var err error
	for attempt := 0; attempt < 30; attempt++ {
		u, err = c.resolveDownloadURL(ctx, item)
		if err == nil {
			break
		}
		var status statusError
		if !errors.Is(err, os.ErrNotExist) && !(errors.As(err, &status) && status.code == 404) && !strings.Contains(err.Error(), "MED-1017") {
			return nil, err
		}
		if attempt < 29 {
			if waitErr := pause(ctx, time.Second); waitErr != nil {
				return nil, waitErr
			}
		}
	}
	if err != nil {
		return nil, err
	}
	session, err := c.getSession(ctx)
	if err != nil {
		return nil, err
	}
	if err = c.throttle(ctx); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	headers(session, req)
	c.originHeaders(req)
	start := off
	skip := int64(0)
	if off >= 2_000_000_000 {
		start = 1_999_000_000
		skip = off - start
	}
	if off > 0 || limit >= 0 {
		if limit > 0 && skip == 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, off+limit-1))
		} else {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
		}
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 && resp.StatusCode != 206 {
		resp.Body.Close()
		return nil, statusError{resp.StatusCode}
	}
	if (off > 0 || limit >= 0) && resp.StatusCode != 206 {
		resp.Body.Close()
		return nil, errors.New("O2 ignored requested byte range")
	}
	if skip > 0 {
		if _, err = io.CopyN(io.Discard, resp.Body, skip); err != nil {
			resp.Body.Close()
			return nil, err
		}
	}
	if limit >= 0 {
		return &limitedReader{Reader: io.LimitReader(resp.Body, limit), Closer: resp.Body}, nil
	}
	return resp.Body, nil
}

func (c *Client) resolveDownloadURL(ctx context.Context, item Item) (string, error) {
	p, err := c.request(ctx, http.MethodPost, "media", url.Values{"action": {"get"}, "origin": {"omh,dropbox"}},
		marshal(map[string]any{"data": map[string]any{"ids": []string{item.ID}, "fields": []string{"url", "size", "origin"}}}))
	if err != nil {
		return "", err
	}
	data := asMap(p["data"])
	var list []any
	for _, n := range []string{"media", "files", "items"} {
		if list = asArray(data[n]); len(list) > 0 {
			break
		}
	}
	if len(list) == 0 {
		return "", os.ErrNotExist
	}
	u := mediaString(asMap(list[0]), "downloadurl", "url", "viewurl", "playbackurl")
	if u == "" {
		return "", os.ErrNotExist
	}
	if !strings.HasPrefix(u, "http") {
		base := stringField(data, "mediaserverurl")
		if base == "" {
			base = strings.TrimSuffix(strings.Split(c.apiURL, "/sapi")[0], "/")
		}
		u = strings.TrimRight(base, "/") + "/" + strings.TrimLeft(u, "/")
	}
	return u, nil
}

type limitedReader struct {
	io.Reader
	io.Closer
}

// Upload sends one stream once. A missing response is resolved by ConfirmUpload,
// never by blindly sending the stream a second time.
func (c *Client) Upload(ctx context.Context, parent, name string, in io.Reader, size int64) (Item, error) {
	before, err := c.Files(ctx, parent)
	if err != nil {
		return Item{}, err
	}
	known := make(map[string]bool, len(before))
	for _, item := range before {
		known[item.ID] = true
	}
	metadata := marshal(map[string]any{"data": map[string]any{"name": name, "size": size, "modificationdate": "", "folderid": idValue(parent), "contenttype": "application/octet-stream"}})
	form, err := os.CreateTemp("", "juicefs-o2-multipart-*")
	if err != nil {
		return Item{}, err
	}
	defer os.Remove(form.Name())
	defer form.Close()
	writer := multipart.NewWriter(form)
	if err := writer.WriteField("data", string(metadata)); err != nil {
		return Item{}, err
	}
	part, err := writer.CreateFormFile("file", name)
	if err != nil {
		return Item{}, err
	}
	written, err := io.Copy(part, in)
	if err != nil {
		return Item{}, err
	}
	if written != size {
		return Item{}, fmt.Errorf("O2 upload input has %d bytes, expected %d", written, size)
	}
	if err = writer.Close(); err != nil {
		return Item{}, err
	}
	stat, err := form.Stat()
	if err != nil {
		return Item{}, err
	}
	if _, err = form.Seek(0, io.SeekStart); err != nil {
		return Item{}, err
	}
	session, err := c.getSession(ctx)
	if err != nil {
		return Item{}, err
	}
	if err = c.throttle(ctx); err != nil {
		return Item{}, err
	}
	params := url.Values{"action": {"save"}, "validationkey": {session.ValidationKey}}
	if size > 200<<20 {
		params.Set("acceptasynchronous", "true")
	}
	u := c.uploadURL + "?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, form)
	if err != nil {
		return Item{}, err
	}
	req.ContentLength = stat.Size()
	headers(session, req)
	c.originHeaders(req)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Accept", "*/*")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	resp, sendErr := c.http.Do(req)
	var uploadID string
	if sendErr == nil {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr == nil {
			var payload map[string]any
			if json.Unmarshal(body, &payload) == nil {
				if apiErr := apiError(payload); apiErr != nil {
					return Item{}, apiErr
				}
				uploadID = uploadResponseID(payload, name)
			}
		}
		if resp.StatusCode == 401 || resp.StatusCode == 403 {
			return Item{}, statusError{resp.StatusCode}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			if resp.StatusCode < 500 {
				return Item{}, statusError{resp.StatusCode}
			}
			sendErr = statusError{resp.StatusCode}
		}
	}
	item, confirmErr := c.ConfirmUpload(ctx, parent, name, size, uploadID, known)
	if confirmErr == nil {
		if !strings.EqualFold(item.Name, name) {
			return Item{}, fmt.Errorf("O2 accepted upload as %q instead of %q (ID %s)", item.Name, name, item.ID)
		}
		if item.Mtime.IsZero() {
			item.Mtime = time.Now()
		}
		return item, nil
	}
	if sendErr != nil {
		return Item{}, fmt.Errorf("upload result ambiguous (%v); confirmation failed: %w", sendErr, confirmErr)
	}
	return Item{}, confirmErr
}

func uploadResponseID(payload map[string]any, name string) string {
	var visit func(any, int) string
	visit = func(raw any, depth int) string {
		if depth > 5 {
			return ""
		}
		switch value := raw.(type) {
		case map[string]any:
			for _, field := range []string{"media", "file", "item", "files", "items", "result", "upload", "data"} {
				if id := visit(value[field], depth+1); id != "" {
					return id
				}
			}
			candidateName := stringField(value, "name", "filename")
			if candidateName == "" || strings.EqualFold(candidateName, name) {
				if id := stringField(value, "id", "mediaid", "mediaId", "fdoid", "uuid"); id != "" {
					return id
				}
			}
		case []any:
			for _, child := range value {
				if id := visit(child, depth+1); id != "" {
					return id
				}
			}
		}
		return ""
	}
	return visit(payload, 0)
}

func (c *Client) ConfirmUpload(ctx context.Context, parent, name string, size int64, uploadID string, known map[string]bool) (Item, error) {
	for attempt := 0; attempt < 96; attempt++ {
		items, err := c.Files(ctx, parent)
		if err == nil {
			var matches []Item
			for _, item := range items {
				if item.Size != size {
					continue
				}
				if uploadID != "" {
					if item.ID == uploadID {
						return item, nil
					}
					continue
				}
				if !known[item.ID] && strings.EqualFold(item.Name, name) {
					matches = append(matches, item)
				}
			}
			if len(matches) == 1 {
				return matches[0], nil
			}
			if len(matches) > 1 {
				return Item{}, fmt.Errorf("multiple new O2 IDs for %q", name)
			}
		}
		if attempt < 95 {
			if err := pause(ctx, 1250*time.Millisecond); err != nil {
				return Item{}, err
			}
		}
	}
	return Item{}, fmt.Errorf("O2 did not confirm upload of %q", name)
}

func (c *Client) Delete(ctx context.Context, item Item) error {
	for attempt := 0; attempt < 12; attempt++ {
		err := c.deleteOnce(ctx, item)
		if err == nil || !strings.Contains(err.Error(), "MED-1017") {
			return err
		}
		if attempt < 11 {
			if waitErr := pause(ctx, time.Second); waitErr != nil {
				return waitErr
			}
		}
	}
	return fmt.Errorf("O2 media ID %s remained unvalidated for deletion", item.ID)
}

func (c *Client) deleteOnce(ctx context.Context, item Item) error {
	if item.Folder {
		files, err := c.Files(ctx, item.ID)
		if err != nil {
			return err
		}
		folders, err := c.Folders(ctx, item.ID)
		if err != nil {
			return err
		}
		if len(files)+len(folders) > 0 {
			return fmt.Errorf("refusing to delete nonempty O2 folder %q", item.Name)
		}
		_, err = c.request(ctx, http.MethodPost, "media/folder", url.Values{"action": {"softdelete"}}, marshal(map[string]any{"data": map[string]any{"ids": []any{idValue(item.ID)}}}))
		return c.confirmGone(ctx, item, err)
	}
	kinds := []string{item.Kind}
	if item.Kind != "file" {
		kinds = append(kinds, "file")
	}
	var last error
	for _, k := range kinds {
		field := map[string]string{"picture": "pictures", "video": "videos", "audio": "audios", "file": "files"}[k]
		if field == "" {
			field = "files"
		}
		_, last = c.request(ctx, http.MethodPost, "media/"+k, url.Values{"action": {"delete"}, "softdelete": {"true"}}, marshal(map[string]any{"data": map[string]any{field: []any{idValue(item.ID)}}}))
		if last == nil || retryable(last) {
			return c.confirmGone(ctx, item, last)
		}
	}
	return last
}

func (c *Client) confirmGone(ctx context.Context, item Item, operationErr error) error {
	observed := false
	for attempt := 0; attempt < 16; attempt++ {
		var items []Item
		var err error
		if item.Folder {
			items, err = c.Folders(ctx, item.ParentID)
		} else {
			items, err = c.Files(ctx, item.ParentID)
		}
		if err == nil {
			found := false
			for _, current := range items {
				if current.ID == item.ID {
					found = true
					observed = true
					break
				}
			}
			if !found && (operationErr == nil || observed) {
				return nil
			}
		}
		if attempt < 15 {
			if err := pause(ctx, time.Second); err != nil {
				return err
			}
		}
	}
	if operationErr != nil {
		return fmt.Errorf("O2 deletion of ID %s was ambiguous: %w", item.ID, operationErr)
	}
	return fmt.Errorf("O2 deletion of ID %s was not confirmed", item.ID)
}
