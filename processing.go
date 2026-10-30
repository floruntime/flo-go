package flo

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ProcessingClient provides stream processing operations.
type ProcessingClient struct {
	client *Client
}

// =============================================================================
// Core Processing Operations
// =============================================================================

// Submit submits a processing job from a YAML definition.
// Returns the server-assigned job ID.
func (p *ProcessingClient) Submit(yaml []byte, opts *ProcessingSubmitOptions) (string, error) {
	if opts == nil {
		opts = &ProcessingSubmitOptions{}
	}
	namespace := p.client.getNamespace(opts.Namespace)

	resp, err := p.client.sendAndCheck(OpProcessingSubmit, namespace, nil, yaml, nil, false)
	if err != nil {
		return "", err
	}

	return string(resp.Data), nil
}

// Status gets the status of a processing job.
func (p *ProcessingClient) Status(jobID string, opts *ProcessingStatusOptions) (*ProcessingStatusResult, error) {
	if opts == nil {
		opts = &ProcessingStatusOptions{}
	}
	namespace := p.client.getNamespace(opts.Namespace)

	resp, err := p.client.sendAndCheck(OpProcessingStatus, namespace, []byte(jobID), nil, nil, true)
	if err != nil {
		return nil, err
	}

	return parseProcessingStatus(resp.Data)
}

// List returns one page of the namespace's processing jobs. Pass the
// result's Cursor in the next call's options to read the next page.
func (p *ProcessingClient) List(opts *ProcessingListOptions) (*ProcessingListResult, error) {
	if opts == nil {
		opts = &ProcessingListOptions{}
	}
	namespace := p.client.getNamespace(opts.Namespace)

	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}

	// Wire format: [limit:u32][cursor...]
	value := make([]byte, 4+len(opts.Cursor))
	binary.LittleEndian.PutUint32(value[0:4], limit)
	if len(opts.Cursor) > 0 {
		copy(value[4:], opts.Cursor)
	}

	resp, err := p.client.sendAndCheck(OpProcessingList, namespace, nil, value, nil, false)
	if err != nil {
		return nil, err
	}

	return parseProcessingList(resp.Data)
}

// Stop gracefully stops a processing job.
func (p *ProcessingClient) Stop(jobID string, opts *ProcessingStopOptions) error {
	if opts == nil {
		opts = &ProcessingStopOptions{}
	}
	namespace := p.client.getNamespace(opts.Namespace)

	_, err := p.client.sendAndCheck(OpProcessingStop, namespace, []byte(jobID), nil, nil, false)
	return err
}

// Cancel force-cancels a processing job.
func (p *ProcessingClient) Cancel(jobID string, opts *ProcessingCancelOptions) error {
	if opts == nil {
		opts = &ProcessingCancelOptions{}
	}
	namespace := p.client.getNamespace(opts.Namespace)

	_, err := p.client.sendAndCheck(OpProcessingCancel, namespace, []byte(jobID), nil, nil, false)
	return err
}

// =============================================================================
// Declarative Sync
// =============================================================================

// ProcessingSyncResult describes what happened during a Sync operation.
type ProcessingSyncResult struct {
	Name  string // Job name from YAML
	JobID string // Server-assigned job ID
}

// Sync performs a declarative sync of a processing job YAML file.
//
// Unlike workflow sync (which has version-based change detection),
// processing jobs have no GetDefinition opcode — each Sync submits
// a new job instance and returns the server-assigned job ID.
func (p *ProcessingClient) Sync(yamlPath string, opts *ProcessingSyncOptions) (*ProcessingSyncResult, error) {
	if opts == nil {
		opts = &ProcessingSyncOptions{}
	}

	yamlBytes, err := os.ReadFile(yamlPath)
	if err != nil {
		return nil, fmt.Errorf("flo: failed to read processing file %s: %w", yamlPath, err)
	}

	return p.SyncBytes(yamlBytes, opts)
}

// SyncBytes is like Sync but accepts raw YAML bytes instead of a file path.
func (p *ProcessingClient) SyncBytes(yaml []byte, opts *ProcessingSyncOptions) (*ProcessingSyncResult, error) {
	if opts == nil {
		opts = &ProcessingSyncOptions{}
	}

	name, err := extractProcessingMeta(yaml)
	if err != nil {
		return nil, err
	}

	jobID, err := p.Submit(yaml, &ProcessingSubmitOptions{Namespace: opts.Namespace})
	if err != nil {
		return nil, fmt.Errorf("flo: failed to sync processing job %q: %w", name, err)
	}

	return &ProcessingSyncResult{Name: name, JobID: jobID}, nil
}

// SyncDir syncs all YAML files in a directory. Returns results for each file.
func (p *ProcessingClient) SyncDir(dir string, opts *ProcessingSyncOptions) ([]*ProcessingSyncResult, error) {
	if opts == nil {
		opts = &ProcessingSyncOptions{}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("flo: failed to read directory %s: %w", dir, err)
	}

	var results []*ProcessingSyncResult
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".yaml") || strings.HasSuffix(name, ".yml") {
			result, err := p.Sync(filepath.Join(dir, name), opts)
			if err != nil {
				return results, fmt.Errorf("flo: failed to sync %s: %w", name, err)
			}
			results = append(results, result)
		}
	}

	return results, nil
}

// =============================================================================
// YAML Metadata Extraction
// =============================================================================

// extractProcessingMeta extracts the job name from processing YAML.
func extractProcessingMeta(data []byte) (string, error) {
	name := extractYAMLField(data, "name")
	if name == "" {
		return "", fmt.Errorf("flo: processing YAML missing required 'name' field")
	}
	return name, nil
}

// =============================================================================
// Wire Format Parsers
// =============================================================================

var processingStatusNames = []string{"running", "stopped", "cancelled", "failed", "completed"}

// parseProcessingStatus parses the binary wire format for processing job status.
//
// Wire format: [job_id_len:u16][job_id][name_len:u16][name][status:u8]
//
//	[batch_size:u32][records_processed:u64][created_at:i64]
//	[reason_len:u16][reason]
func parseProcessingStatus(data []byte) (*ProcessingStatusResult, error) {
	pos := 0
	short := func(field string) error {
		return fmt.Errorf("%w: processing status ends before %s", ErrIncompleteResponse, field)
	}
	readStr := func(field string) (string, error) {
		if pos+2 > len(data) {
			return "", short(field)
		}
		n := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+n > len(data) {
			return "", short(field)
		}
		s := string(data[pos : pos+n])
		pos += n
		return s, nil
	}

	jobID, err := readStr("job_id")
	if err != nil {
		return nil, err
	}
	name, err := readStr("name")
	if err != nil {
		return nil, err
	}

	if pos+1+4+8+8 > len(data) {
		return nil, short("created_at")
	}
	statusByte := data[pos]
	pos++
	if int(statusByte) >= len(processingStatusNames) {
		return nil, fmt.Errorf("%w: processing status %d is not one this client knows", ErrIncompleteResponse, statusByte)
	}
	batchSize := binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4
	recordsProcessed := binary.LittleEndian.Uint64(data[pos : pos+8])
	pos += 8
	createdAt := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
	pos += 8

	reason, err := readStr("failure reason")
	if err != nil {
		return nil, err
	}
	if pos != len(data) {
		return nil, fmt.Errorf("%w: %d bytes after the processing status", ErrIncompleteResponse, len(data)-pos)
	}

	return &ProcessingStatusResult{
		JobID:            jobID,
		Name:             name,
		Status:           processingStatusNames[statusByte],
		BatchSize:        batchSize,
		RecordsProcessed: recordsProcessed,
		CreatedAt:        createdAt,
		FailureReason:    reason,
	}, nil
}

// parseProcessingList reads processing.list's answer:
// [count:u32]([name_len:u16][name][job_id_len:u16][job_id]
// [status_len:u16][status][created_at:i64])*[has_more:u8][cursor_len:u16][cursor].
func parseProcessingList(data []byte) (*ProcessingListResult, error) {
	r := bodyReader{buf: data}
	n := r.u32()
	// Each job takes at least fourteen bytes, so a count past that is a lie.
	if uint64(n)*14 > uint64(len(data)) {
		return nil, ErrIncompleteResponse
	}
	got := &ProcessingListResult{Jobs: make([]*ProcessingListEntry, 0, n)}
	for i := uint32(0); i < n && !r.bad; i++ {
		got.Jobs = append(got.Jobs, &ProcessingListEntry{
			Name:      string(r.bytes16()),
			JobID:     string(r.bytes16()),
			Status:    string(r.bytes16()),
			CreatedAt: int64(r.u64()),
		})
	}
	got.HasMore, got.Cursor = r.pageTrailer()
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}
