// Package source implements input adapters for Mako pipelines.
//
// FileSource reads events from local files or HTTP/HTTPS URLs
// (JSONL, CSV, JSON). Gzip-compressed files (.gz) are transparently
// decompressed during streaming.
package source

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Stefen-Taime/mako/pkg/pipeline"
)

// ═══════════════════════════════════════════
// File Source (JSONL, CSV, JSON + HTTP URLs)
// ═══════════════════════════════════════════

// FileSource reads events from local files or remote HTTP/HTTPS URLs.
// Supports JSONL (.jsonl, .json), CSV (.csv), and line-delimited formats.
// Gzip-compressed files (.gz) are transparently decompressed in streaming
// mode, keeping memory usage constant regardless of file size.
//
// Configuration via YAML:
//
//	source:
//	  type: file
//	  config:
//	    path: /data/events.jsonl                                           # local file
//	    # path: /data/events.jsonl.gz                                     # gzip compressed
//	    # path: /data/events/*.jsonl.gz                                   # glob + gzip
//	    # path: /data/events/*.jsonl                                      # glob pattern
//	    # path: https://raw.githubusercontent.com/user/repo/main/data.json # HTTP URL
//	    format: jsonl                      # jsonl|csv|json (auto-detected from extension)
//	    csv_header: true                   # first line is header (CSV only)
//	    csv_delimiter: ","                 # field separator (CSV only)
//	    watch: false                       # watch for new files (tail -f style)
//	    batch_size: 1000                   # events per batch pushed to channel
type FileSource struct {
	path         string
	format       string // jsonl, csv, json
	csvHeader    bool
	csvDelimiter rune
	watch        bool
	batchSize    int
	config       map[string]any

	eventCh chan *pipeline.Event
	lag     atomic.Int64
	closed  atomic.Bool
	wg      sync.WaitGroup
}

// NewFileSource creates a file source from config.
func NewFileSource(config map[string]any) *FileSource {
	path := strFromConfig(config, "path", "")
	format := strFromConfig(config, "format", "")
	csvHeader := boolFromConfig(config, "csv_header", true)
	csvDelim := strFromConfig(config, "csv_delimiter", ",")
	watch := boolFromConfig(config, "watch", false)
	batchSize := intFromConfig(config, "batch_size", 1000)

	// Auto-detect format from extension if not specified.
	// For .gz files, strip the gz suffix and detect the inner format
	// (e.g. events.csv.gz → csv, data.jsonl.gz → jsonl).
	if format == "" && path != "" {
		name := strings.ToLower(path)
		// Strip .gz to reach the real format extension
		name = strings.TrimSuffix(name, ".gz")
		ext := filepath.Ext(name)
		switch ext {
		case ".csv":
			format = "csv"
		case ".json":
			format = "json"
		case ".jsonl", ".ndjson":
			format = "jsonl"
		case ".parquet":
			format = "parquet"
		default:
			format = "jsonl"
		}
	}

	delim := rune(',')
	if len(csvDelim) > 0 {
		if csvDelim == "\\t" || csvDelim == "tab" {
			delim = '\t'
		} else {
			delim = rune(csvDelim[0])
		}
	}

	chanBuf := intFromConfig(config, "channel_buffer", 10000)

	return &FileSource{
		path:         path,
		format:       format,
		csvHeader:    csvHeader,
		csvDelimiter: delim,
		watch:        watch,
		batchSize:    batchSize,
		config:       config,
		eventCh:      make(chan *pipeline.Event, chanBuf),
	}
}

// isURL returns true if the path looks like an HTTP/HTTPS URL.
func isURL(path string) bool {
	return strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://")
}

// isGzipped returns true if the path ends with .gz (case-insensitive).
func isGzipped(path string) bool {
	return strings.HasSuffix(strings.ToLower(path), ".gz")
}

// Open validates the file source configuration.
func (s *FileSource) Open(ctx context.Context) error {
	if s.path == "" {
		return fmt.Errorf("file source: path is required (set config.path)")
	}

	// HTTP/HTTPS URL — check reachability with a HEAD request
	if isURL(s.path) {
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, s.path, nil)
		if err != nil {
			return fmt.Errorf("file source: invalid url %q: %w", s.path, err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("file source: url unreachable %q: %w", s.path, err)
		}
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			return fmt.Errorf("file source: url %q returned HTTP %d", s.path, resp.StatusCode)
		}
		return nil
	}

	// Check if path contains glob pattern
	if strings.ContainsAny(s.path, "*?[") {
		matches, err := filepath.Glob(s.path)
		if err != nil {
			return fmt.Errorf("file source: invalid glob %q: %w", s.path, err)
		}
		if len(matches) == 0 {
			return fmt.Errorf("file source: no files matched %q", s.path)
		}
		return nil
	}

	// Single file — check existence
	if _, err := os.Stat(s.path); err != nil {
		return fmt.Errorf("file source: %w", err)
	}

	return nil
}

// Read starts reading files and returns the event channel.
func (s *FileSource) Read(ctx context.Context) (<-chan *pipeline.Event, error) {
	s.wg.Add(1)
	go s.readLoop(ctx)
	return s.eventCh, nil
}

// readLoop reads all files/URLs and pushes events to the channel.
func (s *FileSource) readLoop(ctx context.Context) {
	defer s.wg.Done()
	defer func() {
		if s.closed.CompareAndSwap(false, true) {
			close(s.eventCh)
		}
	}()

	// HTTP/HTTPS URL — fetch and read directly
	if isURL(s.path) {
		if err := s.readURL(ctx, s.path); err != nil {
			fmt.Fprintf(os.Stderr, "[file] error reading %s: %v\n", s.path, err)
		}
		return
	}

	// Resolve local file list
	var files []string
	if strings.ContainsAny(s.path, "*?[") {
		matches, err := filepath.Glob(s.path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[file] glob error: %v\n", err)
			return
		}
		files = matches
	} else {
		files = []string{s.path}
	}

	for _, filePath := range files {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := s.readFile(ctx, filePath); err != nil {
			fmt.Fprintf(os.Stderr, "[file] error reading %s: %v\n", filePath, err)
		}
	}
}

// readFile reads a single file and emits events.
// If the file has a .gz extension, it is transparently decompressed
// via gzip streaming — the full file is never loaded into memory.
func (s *FileSource) readFile(ctx context.Context, filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	var r io.Reader = f
	if isGzipped(filePath) {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("gzip open %s: %w", filePath, err)
		}
		defer gz.Close()
		r = gz
	}

	switch s.format {
	case "csv":
		return s.readCSV(ctx, r, filePath)
	case "json":
		return s.readJSON(ctx, r, filePath)
	case "parquet":
		return fmt.Errorf("parquet support requires apache/parquet-go (use jsonl or csv)")
	default: // jsonl, ndjson
		return s.readJSONL(ctx, r, filePath)
	}
}

// readURL fetches data from an HTTP/HTTPS URL and reads it using the
// appropriate format reader (json, csv, jsonl). The response body is
// streamed directly to the reader — no temp file is created.
// If the URL path ends with .gz or the server returns Content-Encoding: gzip,
// the stream is transparently decompressed.
func (s *FileSource) readURL(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	// Disable automatic gzip handling so we can detect .gz URLs ourselves
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch url: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("url returned HTTP %d", resp.StatusCode)
	}

	var r io.Reader = resp.Body
	if isGzipped(url) || resp.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			return fmt.Errorf("gzip open url %s: %w", url, err)
		}
		defer gz.Close()
		r = gz
	}

	switch s.format {
	case "csv":
		return s.readCSV(ctx, r, url)
	case "json":
		return s.readJSON(ctx, r, url)
	default: // jsonl, ndjson
		return s.readJSONL(ctx, r, url)
	}
}

// readJSONL reads newline-delimited JSON.
func (s *FileSource) readJSONL(ctx context.Context, r io.Reader, filePath string) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024) // up to 10MB lines

	var offset int64
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var value map[string]any
		if err := json.Unmarshal(line, &value); err != nil {
			// Wrap non-JSON lines
			value = map[string]any{
				"_raw":   string(line),
				"_error": fmt.Sprintf("json parse: %v", err),
			}
		}

		event := &pipeline.Event{
			Value:     value,
			RawValue:  append([]byte(nil), line...),
			Timestamp: time.Now(),
			Topic:     filePath,
			Partition: 0,
			Offset:    offset,
		}

		select {
		case s.eventCh <- event:
			offset++
			s.lag.Store(int64(len(s.eventCh)))
		case <-ctx.Done():
			return nil
		}
	}

	return scanner.Err()
}

// readCSV reads CSV files. First row can be header.
// RawValue is left nil — serialized lazily via Event.EnsureRawValue() when needed.
func (s *FileSource) readCSV(ctx context.Context, r io.Reader, filePath string) error {
	br := bufio.NewReaderSize(r, 256*1024) // 256 KB read buffer
	reader := csv.NewReader(br)
	reader.Comma = s.csvDelimiter
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	var headers []string
	var offset int64

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "[file] csv parse error at line %d: %v\n", offset+1, err)
			offset++
			continue
		}

		// First row = header
		if headers == nil && s.csvHeader {
			headers = make([]string, len(record))
			copy(headers, record)
			continue
		}

		// Build event value
		value := make(map[string]any, len(record))
		if headers != nil {
			for i, v := range record {
				if i < len(headers) {
					value[headers[i]] = v
				} else {
					value[fmt.Sprintf("col_%d", i)] = v
				}
			}
		} else {
			for i, v := range record {
				value[fmt.Sprintf("col_%d", i)] = v
			}
		}

		// RawValue left nil — serialized lazily by EnsureRawValue() when a sink needs it
		event := &pipeline.Event{
			Value:     value,
			Timestamp: time.Now(),
			Topic:     filePath,
			Partition: 0,
			Offset:    offset,
		}

		select {
		case s.eventCh <- event:
			offset++
			s.lag.Store(int64(len(s.eventCh)))
		case <-ctx.Done():
			return nil
		}
	}

	return nil
}

// readJSON reads a single JSON file (array of objects or single object)
// using a streaming decoder — no io.ReadAll, constant memory for arrays.
func (s *FileSource) readJSON(ctx context.Context, r io.Reader, filePath string) error {
	br := bufio.NewReaderSize(r, 256*1024)
	dec := json.NewDecoder(br)

	// Peek at first token to decide array vs object
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("read json: %w", err)
	}

	var offset int64

	if delim, ok := tok.(json.Delim); ok && delim == '[' {
		// Stream array elements one by one — constant memory
		for dec.More() {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			var value map[string]any
			if err := dec.Decode(&value); err != nil {
				fmt.Fprintf(os.Stderr, "[file] json array element %d: %v\n", offset, err)
				offset++
				continue
			}

			event := &pipeline.Event{
				Value:     value,
				Timestamp: time.Now(),
				Topic:     filePath,
				Partition: 0,
				Offset:    offset,
			}

			select {
			case s.eventCh <- event:
				offset++
				s.lag.Store(int64(len(s.eventCh)))
			case <-ctx.Done():
				return nil
			}
		}
		// consume closing ']'
		if _, err := dec.Token(); err != nil && err != io.EOF {
			return fmt.Errorf("read json closing bracket: %w", err)
		}
	} else {
		// First token was '{' or a scalar — reparse as single object.
		// We need to re-read because the decoder already consumed the token.
		// Use a small buffer: concatenate the already-consumed token + rest.
		remaining, err := io.ReadAll(dec.Buffered())
		if err != nil {
			return fmt.Errorf("read json buffered: %w", err)
		}
		rest, err := io.ReadAll(br)
		if err != nil {
			return fmt.Errorf("read json rest: %w", err)
		}
		// Reconstruct: the token was '{', so prepend it
		full := append([]byte("{"), remaining...)
		full = append(full, rest...)

		var value map[string]any
		if err := json.Unmarshal(full, &value); err != nil {
			return fmt.Errorf("parse json object: %w", err)
		}

		event := &pipeline.Event{
			Value:     value,
			Timestamp: time.Now(),
			Topic:     filePath,
			Partition: 0,
			Offset:    0,
		}

		select {
		case s.eventCh <- event:
		case <-ctx.Done():
			return nil
		}
	}

	return nil
}

// Close stops the file source.
func (s *FileSource) Close() error {
	s.wg.Wait()
	return nil
}

// Lag returns the channel backlog.
func (s *FileSource) Lag() int64 {
	return s.lag.Load()
}

// ═══════════════════════════════════════════
// Config helpers
// ═══════════════════════════════════════════

func strFromConfig(cfg map[string]any, key, defaultVal string) string {
	if cfg == nil {
		return defaultVal
	}
	if v, ok := cfg[key].(string); ok && v != "" {
		return v
	}
	return defaultVal
}

func boolFromConfig(cfg map[string]any, key string, defaultVal bool) bool {
	if cfg == nil {
		return defaultVal
	}
	if v, ok := cfg[key].(bool); ok {
		return v
	}
	return defaultVal
}

func intFromConfig(cfg map[string]any, key string, defaultVal int) int {
	if cfg == nil {
		return defaultVal
	}
	switch v := cfg[key].(type) {
	case int:
		return v
	case float64:
		return int(v)
	case int64:
		return int(v)
	}
	return defaultVal
}
