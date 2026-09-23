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

// o2cloud-lab runs a bounded O2 Cloud probe inside JUICEFS-O2-LAB.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	o2 "github.com/juicedata/juicefs/pkg/object/o2cloud"
)

func run() error {
	bench := flag.Bool("bench", false, "upload, verify, and remove one 4 MiB and one 16 MiB object")
	audit := flag.Bool("audit", false, "check for leftover JuiceFS lab and objbench folders")
	tps := flag.Float64("tps", 2, "maximum O2 requests per second")
	flag.Parse()
	file := os.Getenv("O2CLOUD_SESSION_FILE")
	if file == "" {
		return errors.New("set O2CLOUD_SESSION_FILE to the secondary account session file")
	}
	client, err := o2.New(o2.Config{SessionFile: file, TPS: *tps})
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	root, err := client.Root(ctx)
	if err != nil {
		return err
	}
	lab, err := client.Find(ctx, root.ID, "JUICEFS-O2-LAB", true)
	if err != nil {
		return fmt.Errorf("secondary account has no JUICEFS-O2-LAB root folder: %w", err)
	}
	fmt.Printf("Found JUICEFS-O2-LAB (O2 ID %s)\n", lab.ID)
	if *audit {
		folders, err := client.Folders(ctx, lab.ID)
		if err != nil {
			return err
		}
		files, err := client.Files(ctx, lab.ID)
		if err != nil {
			return err
		}
		fmt.Printf("LAB_ROOT_FOLDERS=%d LAB_ROOT_FILES=%d\n", len(folders), len(files))
		var remaining int
		for _, folder := range folders {
			if strings.HasPrefix(folder.Name, "juicefs-lab-") || strings.HasPrefix(folder.Name, "__juicefs_benchmark_") {
				remaining++
			}
		}
		fmt.Printf("LAB_TEST_FOLDERS_REMAINING=%d\n", remaining)
		if remaining != 0 {
			return fmt.Errorf("%d JuiceFS test folders remain in JUICEFS-O2-LAB", remaining)
		}
		return nil
	}
	if !*bench {
		return nil
	}
	if err := os.Setenv("O2CLOUD_TPS", fmt.Sprint(*tps)); err != nil {
		return err
	}
	store, err := object.CreateStorage("o2cloud", "JUICEFS-O2-LAB", "", "", "")
	if err != nil {
		return err
	}
	defer object.Shutdown(store)
	prefix := fmt.Sprintf("juicefs-lab-%d/", time.Now().UnixNano())
	if err := store.Put(ctx, prefix, bytes.NewReader(nil)); err != nil {
		return err
	}
	var uploaded []string
	defer func() {
		clean := true
		for _, key := range uploaded {
			if deleteErr := store.Delete(ctx, key); deleteErr != nil {
				clean = false
				fmt.Fprintln(os.Stderr, "lab cleanup:", deleteErr)
			}
		}
		if clean {
			if err := store.Delete(ctx, prefix); err != nil {
				fmt.Fprintln(os.Stderr, "lab cleanup:", err)
			}
		}
	}()
	for _, size := range []int{4 << 20, 16 << 20} {
		payload := make([]byte, size)
		if _, err = rand.Read(payload); err != nil {
			return err
		}
		key := prefix + fmt.Sprintf("block-%dMiB", size>>20)
		start := time.Now()
		if err := store.Put(ctx, key, bytes.NewReader(payload)); err != nil {
			return err
		}
		uploaded = append(uploaded, key)
		putDuration := time.Since(start)
		head, err := store.Head(ctx, key)
		if err != nil || head.Size() != int64(size) {
			return fmt.Errorf("head %s: %v", key, err)
		}
		start = time.Now()
		r, err := store.Get(ctx, key, 0, -1)
		if err != nil {
			return err
		}
		got, readErr := io.ReadAll(r)
		closeErr := r.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		getDuration := time.Since(start)
		if sha256.Sum256(payload) != sha256.Sum256(got) {
			return fmt.Errorf("checksum mismatch for %s", key)
		}
		ranged, err := store.Get(ctx, key, 1<<20, 1<<20)
		if err != nil {
			return err
		}
		part, readErr := io.ReadAll(ranged)
		closeErr = ranged.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		if !bytes.Equal(part, payload[1<<20:2<<20]) {
			return fmt.Errorf("range mismatch for %s", key)
		}
		fmt.Printf("%d MiB: put %.2fs (%.2f MiB/s), get %.2fs (%.2f MiB/s), SHA-256 and range OK\n",
			size>>20, putDuration.Seconds(), float64(size)/float64(1<<20)/putDuration.Seconds(),
			getDuration.Seconds(), float64(size)/float64(1<<20)/getDuration.Seconds())
	}
	listed, _, _, err := store.List(ctx, prefix, "", "", "", 10, false)
	if err != nil {
		return err
	}
	if len(listed) < len(uploaded) {
		return fmt.Errorf("list returned %d objects, expected %d", len(listed), len(uploaded))
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
