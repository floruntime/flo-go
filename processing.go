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

// List lists processing jobs.
func (p *ProcessingClient) List(opts *ProcessingListOptions) ([]*ProcessingListEntry, error) {
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

// Savepoint triggers a savepoint for a processing job.
// Returns the savepoint ID.
func (p *ProcessingClient) Savepoint(jobID string, opts *ProcessingSavepointOptions) (string, error) {
	if opts == nil {
		opts = &ProcessingSavepointOptions{}
	}
	namespace := p.client.getNamespace(opts.Namespace)

	resp, err := p.client.sendAndCheck(OpProcessingSavepoint, namespace, []byte(jobID), nil, nil, false)
	if err != nil {
		return "", err
	}

	return string(resp.Data), nil
}

// Restore restores a processing job from a savepoint.
func (p *ProcessingClient) Restore(jobID string, savepointID string, opts *ProcessingRestoreOptions) error {
	if opts == nil {
		opts = &ProcessingRestoreOptions{}
	}
	namespace := p.client.getNamespace(opts.Namespace)

	_, err := p.client.sendAndCheck(OpProcessingRestore, namespace, []byte(jobID), []byte(savepointID), nil, false)
	return err
}

// Rescale changes the parallelism of a processing job.
func (p *ProcessingClient) Rescale(jobID string, parallelism uint32, opts *ProcessingRescaleOptions) error {
	if opts == nil {
		opts = &ProcessingRescaleOptions{}
	}
	namespace := p.client.getNamespace(opts.Namespace)

	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, parallelism)

	_, err := p.client.sendAndCheck(OpProcessingRescale, namespace, []byte(jobID), value, nil, false)
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
//	[parallelism:u32][batch_size:u32][records_processed:u64][created_at:i64]
func parseProcessingStatus(data []byte) (*ProcessingStatusResult, error) {
	pos := 0

	readU16 := func() (int, error) {
		if pos+2 > len(data) {
			return 0, fmt.Errorf("unexpected end of processing status data")
		}
		v := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2
		return v, nil
	}
	readStr := func() (string, error) {
		n, err := readU16()
		if err != nil {
			return "", err
		}
		if pos+n > len(data) {
			return "", fmt.Errorf("unexpected end of processing status data")
		}
		s := string(data[pos : pos+n])
		pos += n
		return s, nil
	}

	jobID, err := readStr()
	if err != nil {
		return nil, fmt.Errorf("parse processing status job_id: %w", err)
	}

	name, err := readStr()
	if err != nil {
		return nil, fmt.Errorf("parse processing status name: %w", err)
	}

	if pos >= len(data) {
		return nil, fmt.Errorf("unexpected end of processing status data at status byte")
	}
	statusByte := data[pos]
	pos++
	statusStr := fmt.Sprintf("unknown(%d)", statusByte)
	if int(statusByte) < len(processingStatusNames) {
		statusStr = processingStatusNames[statusByte]
	}

	if pos+4 > len(data) {
		return nil, fmt.Errorf("unexpected end of processing status data at parallelism")
	}
	parallelism := binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4

	if pos+4 > len(data) {
		return nil, fmt.Errorf("unexpected end of processing status data at batch_size")
	}
	batchSize := binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4

	if pos+8 > len(data) {
		return nil, fmt.Errorf("unexpected end of processing status data at records_processed")
	}
	recordsProcessed := binary.LittleEndian.Uint64(data[pos : pos+8])
	pos += 8

	if pos+8 > len(data) {
		return nil, fmt.Errorf("unexpected end of processing status data at created_at")
	}
	createdAt := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))

	return &ProcessingStatusResult{
		JobID:            jobID,
		Name:             name,
		Status:           statusStr,
		Parallelism:      parallelism,
		BatchSize:        batchSize,
		RecordsProcessed: recordsProcessed,
		CreatedAt:        createdAt,
	}, nil
}

// parseProcessingList parses the binary wire format for processing job list.
//
// Wire format: [count:u32]([name_len:u16][name][job_id_len:u16][job_id]
//
//	[status_len:u16][status][parallelism:u32][created_at:i64])*
//	[has_more:u8][cursor_len:u16]
func parseProcessingList(data []byte) ([]*ProcessingListEntry, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("processing list data too short")
	}

	count := binary.LittleEndian.Uint32(data[0:4])
	pos := 4
	results := make([]*ProcessingListEntry, 0, count)

	readU16 := func() (int, error) {
		if pos+2 > len(data) {
			return 0, fmt.Errorf("unexpected end of processing list data")
		}
		v := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2
		return v, nil
	}
	readStr := func() (string, error) {
		n, err := readU16()
		if err != nil {
			return "", err
		}
		if pos+n > len(data) {
			return "", fmt.Errorf("unexpected end of processing list data")
		}
		s := string(data[pos : pos+n])
		pos += n
		return s, nil
	}

	for i := uint32(0); i < count; i++ {
		name, err := readStr()
		if err != nil {
			return nil, fmt.Errorf("parse processing list entry %d name: %w", i, err)
		}

		jobID, err := readStr()
		if err != nil {
			return nil, fmt.Errorf("parse processing list entry %d job_id: %w", i, err)
		}

		status, err := readStr()
		if err != nil {
			return nil, fmt.Errorf("parse processing list entry %d status: %w", i, err)
		}

		if pos+4 > len(data) {
			return nil, fmt.Errorf("unexpected end of processing list data at parallelism for entry %d", i)
		}
		parallelism := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4

		if pos+8 > len(data) {
			return nil, fmt.Errorf("unexpected end of processing list data at created_at for entry %d", i)
		}
		createdAt := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8

		results = append(results, &ProcessingListEntry{
			Name:        name,
			JobID:       jobID,
			Status:      status,
			Parallelism: parallelism,
			CreatedAt:   createdAt,
		})
	}

	return results, nil
}
