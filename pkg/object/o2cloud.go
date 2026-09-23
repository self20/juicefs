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
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	o2 "github.com/juicedata/juicefs/pkg/object/o2cloud"
)

type o2Storage struct {
	DefaultObjectStorage
	client   *o2.Client
	rootName string
	rootID   string
	mu       sync.Mutex
	folderMu sync.Mutex
	keyLocks [64]sync.Mutex
	recent   map[string]o2.Item
	pending  map[string]bool
	folders  map[string]o2.Item
}

func (s *o2Storage) String() string { return "o2cloud://" + s.rootName + "/" }
func (s *o2Storage) Shutdown()      { s.client.Close() }

func cleanO2Key(key string) ([]string, error) {
	if key == "" {
		return nil, nil
	}
	if strings.HasPrefix(key, "/") || strings.Contains(key, "\\") {
		return nil, fmt.Errorf("invalid O2 key %q", key)
	}
	parts := strings.Split(strings.TrimSuffix(key, "/"), "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return nil, fmt.Errorf("invalid O2 key %q", key)
		}
	}
	return parts, nil
}

func (s *o2Storage) folder(ctx context.Context, parts []string, create bool) (string, error) {
	s.folderMu.Lock()
	defer s.folderMu.Unlock()
	id := s.rootID
	key := ""
	for _, part := range parts {
		key += part + "/"
		s.mu.Lock()
		cached, ok := s.folders[key]
		s.mu.Unlock()
		if ok {
			id = cached.ID
			continue
		}
		var item o2.Item
		var err error
		if create {
			item, err = s.client.EnsureFolder(ctx, id, part)
		} else {
			item, err = s.client.Find(ctx, id, part, true)
		}
		if err != nil {
			return "", err
		}
		id = item.ID
		s.mu.Lock()
		if s.folders == nil {
			s.folders = make(map[string]o2.Item)
		}
		s.folders[key] = item
		s.mu.Unlock()
	}
	return id, nil
}

func (s *o2Storage) Create(ctx context.Context) error {
	root, err := s.client.Root(ctx)
	if err != nil {
		return err
	}
	item, err := s.client.Find(ctx, root.ID, s.rootName, true)
	if err != nil {
		return fmt.Errorf("O2 root folder %q must already exist: %w", s.rootName, err)
	}
	s.rootID = item.ID
	return nil
}

func (s *o2Storage) find(ctx context.Context, key string) (o2.Item, error) {
	parts, err := cleanO2Key(key)
	if err != nil {
		return o2.Item{}, err
	}
	if len(parts) == 0 {
		return o2.Item{}, os.ErrNotExist
	}
	if strings.HasSuffix(key, "/") {
		s.mu.Lock()
		item, ok := s.folders[key]
		s.mu.Unlock()
		if ok {
			return item, nil
		}
	}
	if !strings.HasSuffix(key, "/") {
		s.mu.Lock()
		item, ok := s.recent[key]
		if ok && time.Since(item.Mtime) > 24*time.Hour {
			delete(s.recent, key)
			ok = false
		}
		s.mu.Unlock()
		if ok {
			return item, nil
		}
	}
	parent, err := s.folder(ctx, parts[:len(parts)-1], false)
	if err != nil {
		return o2.Item{}, err
	}
	return s.client.Find(ctx, parent, parts[len(parts)-1], strings.HasSuffix(key, "/"))
}

func (s *o2Storage) Head(ctx context.Context, key string) (Object, error) {
	item, err := s.find(ctx, key)
	if err != nil {
		return nil, err
	}
	return &obj{key: key, size: item.Size, mtime: item.Mtime, isDir: item.Folder}, nil
}

func (s *o2Storage) Get(ctx context.Context, key string, off, limit int64, getters ...AttrGetter) (io.ReadCloser, error) {
	if off < 0 || limit < -1 {
		return nil, fmt.Errorf("invalid O2 range %d,%d", off, limit)
	}
	item, err := s.find(ctx, key)
	if err != nil {
		return nil, err
	}
	if item.Folder {
		return nil, fmt.Errorf("%s is a directory", key)
	}
	return s.client.Download(ctx, item, off, limit)
}

func (s *o2Storage) Put(ctx context.Context, key string, in io.Reader, getters ...AttrGetter) error {
	parts, err := cleanO2Key(key)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		return errors.New("empty O2 key")
	}
	lock := &s.keyLocks[int(sha256.Sum256([]byte(key))[0])%len(s.keyLocks)]
	lock.Lock()
	defer lock.Unlock()
	if strings.HasSuffix(key, "/") {
		_, err = s.folder(ctx, parts, true)
		return err
	}
	parent, err := s.folder(ctx, parts[:len(parts)-1], true)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp("", "juicefs-o2-put-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	size, err := io.Copy(tmp, io.LimitReader(in, (1<<30)+1))
	if err != nil {
		return err
	}
	if size > 1<<30 {
		return errors.New("O2 single upload exceeds 1 GiB")
	}
	item, err := s.find(ctx, key)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil {
		if item.Size != size {
			return fmt.Errorf("O2 key %q already exists with different size", key)
		}
		if _, err = tmp.Seek(0, io.SeekStart); err != nil {
			return err
		}
		localHash := sha256.New()
		if _, err = io.Copy(localHash, tmp); err != nil {
			return err
		}
		reader, readErr := s.client.Download(ctx, item, 0, -1)
		if readErr != nil {
			return readErr
		}
		remoteHash := sha256.New()
		remoteSize, readErr := io.Copy(remoteHash, reader)
		closeErr := reader.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		if remoteSize == size && string(remoteHash.Sum(nil)) == string(localHash.Sum(nil)) {
			s.mu.Lock()
			delete(s.pending, key)
			s.mu.Unlock()
			return nil
		}
		return fmt.Errorf("O2 key %q already exists with different content", key)
	}
	s.mu.Lock()
	if s.pending[key] {
		s.mu.Unlock()
		return fmt.Errorf("O2 upload of %q is unconfirmed; refusing to resend", key)
	}
	if s.pending == nil {
		s.pending = make(map[string]bool)
	}
	s.pending[key] = true
	s.mu.Unlock()
	if _, err = tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	item, err = s.client.Upload(ctx, parent, parts[len(parts)-1], tmp, size)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.recent == nil {
		s.recent = make(map[string]o2.Item)
	}
	s.recent[key] = item
	delete(s.pending, key)
	s.mu.Unlock()
	return nil
}

func (s *o2Storage) Delete(ctx context.Context, key string, getters ...AttrGetter) error {
	lock := &s.keyLocks[int(sha256.Sum256([]byte(key))[0])%len(s.keyLocks)]
	lock.Lock()
	defer lock.Unlock()
	item, err := s.find(ctx, key)
	if errors.Is(err, os.ErrNotExist) {
		s.mu.Lock()
		pending := s.pending[key]
		s.mu.Unlock()
		if pending {
			return fmt.Errorf("O2 upload of %q is unconfirmed; refusing to delete by name", key)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if err = s.client.Delete(ctx, item); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.recent, key)
	delete(s.pending, key)
	delete(s.folders, key)
	s.mu.Unlock()
	return nil
}

func (s *o2Storage) recentObjects() []Object {
	s.mu.Lock()
	defer s.mu.Unlock()
	objects := make([]Object, 0, len(s.recent))
	for key, item := range s.recent {
		if time.Since(item.Mtime) > 24*time.Hour {
			delete(s.recent, key)
			continue
		}
		objects = append(objects, &obj{key: key, size: item.Size, mtime: item.Mtime})
	}
	return objects
}

func (s *o2Storage) allObjects(ctx context.Context) ([]Object, error) {
	var all []Object
	if err := s.walk(ctx, s.rootID, "", &all); err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(all))
	for _, item := range all {
		seen[item.Key()] = true
	}
	for _, item := range s.recentObjects() {
		if !seen[item.Key()] {
			all = append(all, item)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Key() < all[j].Key() })
	return all, nil
}

func (s *o2Storage) walk(ctx context.Context, id, prefix string, output *[]Object) error {
	files, err := s.client.Files(ctx, id)
	if err != nil {
		return err
	}
	for _, item := range files {
		*output = append(*output, &obj{key: prefix + item.Name, size: item.Size, mtime: item.Mtime})
	}
	folders, err := s.client.Folders(ctx, id)
	if err != nil {
		return err
	}
	for _, folder := range folders {
		key := prefix + folder.Name + "/"
		*output = append(*output, &obj{key: key, isDir: true, mtime: folder.Mtime})
		if err = s.walk(ctx, folder.ID, key, output); err != nil {
			return err
		}
	}
	return nil
}

func (s *o2Storage) List(ctx context.Context, prefix, startAfter, token, delimiter string, limit int64, followLink bool) ([]Object, bool, string, error) {
	if limit <= 0 {
		return nil, false, "", errors.New("O2 list limit must be positive")
	}
	if delimiter != "" && delimiter != "/" {
		return nil, false, "", notSupported
	}
	if token != "" {
		startAfter = token
	}
	all, err := s.allObjects(ctx)
	if err != nil {
		return nil, false, "", err
	}
	result := make([]Object, 0)
	seenDirs := map[string]bool{}
	for _, item := range all {
		key := item.Key()
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if delimiter == "/" {
			rest := strings.TrimPrefix(key, prefix)
			if index := strings.Index(rest, "/"); index >= 0 {
				key = prefix + rest[:index+1]
				if seenDirs[key] {
					continue
				}
				seenDirs[key] = true
				if item.Key() != key {
					item = &obj{key: key, isDir: true}
				}
			}
		}
		if key <= startAfter {
			continue
		}
		result = append(result, item)
		if int64(len(result)) > limit {
			break
		}
	}
	hasMore := int64(len(result)) > limit
	if hasMore {
		result = result[:limit]
	}
	next := ""
	if hasMore {
		next = result[len(result)-1].Key()
	}
	return result, hasMore, next, nil
}

func (s *o2Storage) ListAll(ctx context.Context, prefix, marker string, followLink bool) (<-chan Object, error) {
	all, err := s.allObjects(ctx)
	if err != nil {
		return nil, err
	}
	ch := make(chan Object, 1000)
	go func() {
		defer close(ch)
		for _, item := range all {
			if !strings.HasPrefix(item.Key(), prefix) || item.Key() <= marker {
				continue
			}
			select {
			case ch <- item:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

func newO2Cloud(endpoint, accessKey, secretKey, token string) (ObjectStorage, error) {
	root := strings.Trim(endpoint, "/")
	if root == "" || strings.Contains(root, "/") || root == "." || root == ".." {
		return nil, fmt.Errorf("O2 endpoint must be one existing root folder name")
	}
	tps := 2.0
	if raw := os.Getenv("O2CLOUD_TPS"); raw != "" {
		var err error
		tps, err = strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid O2CLOUD_TPS: %w", err)
		}
	}
	retries := 3
	if raw := os.Getenv("O2CLOUD_RETRIES"); raw != "" {
		var err error
		retries, err = strconv.Atoi(raw)
		if err != nil || retries < 0 {
			return nil, fmt.Errorf("invalid O2CLOUD_RETRIES %q", raw)
		}
	}
	cfg := o2.Config{SessionFile: os.Getenv("O2CLOUD_SESSION_FILE"), User: accessKey, Password: secretKey, TPS: tps,
		Retries: retries, APIURL: os.Getenv("O2CLOUD_API_URL"), UploadURL: os.Getenv("O2CLOUD_UPLOAD_URL")}
	client, err := o2.New(cfg)
	if err != nil {
		return nil, err
	}
	s := &o2Storage{client: client, rootName: root}
	if err = s.Create(context.Background()); err != nil {
		client.Close()
		return nil, err
	}
	return s, nil
}

func init() { Register("o2cloud", newO2Cloud) }

var _ ObjectStorage = (*o2Storage)(nil)
