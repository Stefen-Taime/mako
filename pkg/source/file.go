// Package source implements input adapters for Mako pipelines.
//
// FileSource reads events from local files (JSONL, CSV, Parquet).
package source

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Stefen-Taime/mako/pkg/pipeline"
)

// ═══════════════════════════════════════════
// File Source (JSONL, CSV, Parquet)
// ═══════════════════════════════════════════

// FileSource reads events from local files.
// Supports JSONL (.jsonl, .json), CSV (.csv), and line-delimited formats.
//
// Configuration via YAML:
//
//	source:
//	  type: file
//	  config:
//	    path: /data/events.jsonl          # single file
//	    # path: /data/events/*.jsonl      # glob pattern
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

	// Auto-detect format from extension if not specified
	if format == "" && path != "" {
		ext := strings.ToLower(filepath.Ext(path))
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

	return &FileSource{
		path:         path,
		format:       format,
		csvHeader:    csvHeader,
		csvDelimiter: delim,
		watch:        watch,
		batchSize:    batchSize,
		config:       config,
		eventCh:      make(chan *pipeline.Event, 1000),
	}
}

// Open validates the file source configuration.
func (s *FileSource) Open(ctx context.Context) error {
	if s.path == "" {
		return fmt.Errorf("file source: path is required (set config.path)")
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

// readLoop reads all files and pushes events to the channel.
func (s *FileSource) readLoop(ctx context.Context) {
	defer s.wg.Done()
	defer func() {
		if s.closed.CompareAndSwap(false, true) {
			close(s.eventCh)
		}
	}()

	// Resolve file list
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
func (s *FileSource) readFile(ctx context.Context, filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	switch s.format {
	case "csv":
		return s.readCSV(ctx, f, filePath)
	case "json":
		return s.readJSON(ctx, f, filePath)
	case "parquet":
		return fmt.Errorf("parquet support requires apache/parquet-go (use jsonl or csv)")
	default: // jsonl, ndjson
		return s.readJSONL(ctx, f, filePath)
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
func (s *FileSource) readCSV(ctx context.Context, r io.Reader, filePath string) error {
	reader := csv.NewReader(r)
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

		rawJSON, _ := json.Marshal(value)

		event := &pipeline.Event{
			Value:     value,
			RawValue:  rawJSON,
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

// readJSON reads a single JSON file (array of objects or single object).
func (s *FileSource) readJSON(ctx context.Context, r io.Reader, filePath string) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("read json: %w", err)
	}

	// Trim whitespace
	trimmed := strings.TrimSpace(string(data))
	var offset int64

	if strings.HasPrefix(trimmed, "[") {
		// Array of objects
		var records []map[string]any
		if err := json.Unmarshal(data, &records); err != nil {
			return fmt.Errorf("parse json array: %w", err)
		}

		for _, value := range records {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			rawJSON, _ := json.Marshal(value)
			event := &pipeline.Event{
				Value:     value,
				RawValue:  rawJSON,
				Timestamp: time.Now(),
				Topic:     filePath,
				Partition: 0,
				Offset:    offset,
			}

			select {
			case s.eventCh <- event:
				offset++
			case <-ctx.Done():
				return nil
			}
		}
	} else {
		// Single object
		var value map[string]any
		if err := json.Unmarshal(data, &value); err != nil {
			return fmt.Errorf("parse json object: %w", err)
		}

		event := &pipeline.Event{
			Value:     value,
			RawValue:  data,
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
