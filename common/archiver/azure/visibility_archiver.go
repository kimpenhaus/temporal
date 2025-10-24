package azure

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/searchattribute"
)

type (
	visibilityArchiver struct {
		containerClient *container.Client
		config          *config.AzureArchiver
		queryParser     QueryParser
		metricsHandler  metrics.Handler
		logger          log.Logger
	}

	queryVisibilityToken struct {
		Marker string `json:"marker"`
	}
)

const (
	errEncodeVisibilityRecord = "failed to encode visibility record"
	errWriteVisibilityBlob    = "failed to write visibility record to blob storage"
)

// NewVisibilityArchiver creates a new Azure blob visibility archiver
func NewVisibilityArchiver(
	logger log.Logger,
	metricsHandler metrics.Handler,
	config *config.AzureArchiver,
) (archiver.VisibilityArchiver, error) {
	containerClient, err := CreateContainerClient(context.TODO(), config)
	if err != nil {
		return nil, err
	}

	return &visibilityArchiver{
		containerClient: containerClient,
		config:          config,
		queryParser:     NewQueryParser(),
		metricsHandler:  metricsHandler,
		logger:          logger,
	}, nil
}

// Archive archives the workflow visibility record to Azure blob storage
func (v *visibilityArchiver) Archive(ctx context.Context, URI archiver.URI, request *archiverspb.VisibilityRecord, opts ...archiver.ArchiveOption) (err error) {
	handler := v.metricsHandler.WithTags(metrics.OperationTag(metrics.VisibilityArchiverScope), metrics.NamespaceTag(request.Namespace))
	featureCatalog := archiver.GetFeatureCatalog(opts...)
	startTime := time.Now().UTC()
	logger := archiver.TagLoggerWithArchiveVisibilityRequestAndURI(v.logger, request, URI.String())
	archiveFailReason := ""

	defer func() {
		metrics.ServiceLatency.With(handler).Record(time.Since(startTime))
		if err != nil {
			if isRetryableError(err) {
				metrics.VisibilityArchiverArchiveTransientErrorCount.With(handler).Record(1)
				logger.Error(archiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason(archiveFailReason), tag.Error(err))
			} else {
				metrics.VisibilityArchiverArchiveNonRetryableErrorCount.With(handler).Record(1)
				logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiveFailReason), tag.Error(err))
				if featureCatalog.NonRetryableError != nil {
					err = featureCatalog.NonRetryableError()
				}
			}
		}
	}()

	if err := v.ValidateURI(URI); err != nil {
		archiveFailReason = archiver.ErrReasonInvalidURI
		return err
	}

	if err := archiver.ValidateVisibilityArchivalRequest(request); err != nil {
		archiveFailReason = archiver.ErrReasonInvalidArchiveRequest
		return err
	}

	// Serialize visibility record to JSON
	data, err := json.Marshal(request)
	if err != nil {
		archiveFailReason = errEncodeVisibilityRecord
		return err
	}

	// Construct blob key for visibility record
	blobKey := constructVisibilityKey(
		URI.Path(),
		request.GetNamespaceId(),
		request.WorkflowTypeName,
		request.GetWorkflowId(),
		request.GetRunId(),
		request.CloseTime.AsTime(),
	)

	// Upload to Azure Blob Storage
	_, err = v.containerClient.NewBlockBlobClient(blobKey).UploadBuffer(
		ctx, data, &azblob.UploadBufferOptions{
			BlockSize:   4 * 1024 * 1024,
			Concurrency: 3,
		})

	if err != nil {
		archiveFailReason = errWriteVisibilityBlob
		return err
	}

	metrics.VisibilityArchiveSuccessCount.With(handler).Record(1)
	return nil
}

// Query searches for archived visibility records
func (v *visibilityArchiver) Query(
	ctx context.Context,
	URI archiver.URI,
	request *archiver.QueryVisibilityRequest,
	saTypeMap searchattribute.NameTypeMap,
) (*archiver.QueryVisibilityResponse, error) {
	handler := v.metricsHandler.WithTags(metrics.OperationTag(metrics.VisibilityArchiverScope))
	startTime := time.Now().UTC()

	defer func() {
		handler.Timer(metrics.ServiceLatency.Name()).Record(time.Since(startTime))
	}()

	logger := log.With(v.logger,
		tag.ArchivalURI(URI.String()),
		tag.ArchivalArchiveFailReason("query visibility"),
	)

	if err := v.ValidateURI(URI); err != nil {
		logger.Error("Failed to validate URI", tag.Error(err))
		return nil, serviceerror.NewInvalidArgument(err.Error())
	}

	if err := archiver.ValidateQueryRequest(request); err != nil {
		logger.Error("Failed to validate request", tag.Error(err))
		return nil, serviceerror.NewInvalidArgument(err.Error())
	}

	// Parse query
	parsedQuery, err := v.queryParser.Parse(request.Query)
	if err != nil {
		logger.Error("Failed to parse query", tag.Error(err))
		return nil, serviceerror.NewInvalidArgument(err.Error())
	}

	// Handle pagination token
	var token *queryVisibilityToken
	if len(request.NextPageToken) > 0 {
		token, err = deserializeQueryVisibilityToken(request.NextPageToken)
		if err != nil {
			logger.Error("Failed to deserialize page token", tag.Error(err))
			return nil, serviceerror.NewInvalidArgument("invalid page token")
		}
	} else {
		token = &queryVisibilityToken{
			Marker: "",
		}
	}

	// Construct prefix for listing blobs
	prefix := constructVisibilitySearchPrefix(
		URI.Path(),
		request.NamespaceID,
		parsedQuery,
	)

	// List blobs with a prefix
	pager := v.containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix:     to.Ptr(prefix),
		Marker:     to.Ptr(token.Marker),
		MaxResults: to.Ptr(int32(request.PageSize)),
	})

	var executions []*archiverspb.VisibilityRecord
	var nextMarker string

	if pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			logger.Error("Failed to list blobs", tag.Error(err))
			return nil, err
		}

		for _, blobItem := range page.Segment.BlobItems {
			// Download and parse each blob
			downloadResp, err := v.containerClient.NewBlockBlobClient(*blobItem.Name).DownloadStream(ctx, nil)
			if err != nil {
				logger.Warn("Failed to download blob", tag.Error(err))
				continue
			}

			data, err := io.ReadAll(downloadResp.Body)
			closeErr := downloadResp.Body.Close()
			if err != nil {
				logger.Warn("Failed to read blob content", tag.Error(err))
				continue
			}
			if closeErr != nil {
				logger.Warn("Failed to close blob stream", tag.Error(closeErr))
				continue
			}

			var record archiverspb.VisibilityRecord
			if err := json.Unmarshal(data, &record); err != nil {
				logger.Warn("Failed to deserialize visibility record", tag.Error(err))
				continue
			}

			// Apply query filters
			if matchesQuery(&record, parsedQuery) {
				executions = append(executions, &record)
			}

			if len(executions) >= request.PageSize {
				break
			}
		}

		if page.NextMarker != nil && *page.NextMarker != "" {
			nextMarker = *page.NextMarker
		}
	}

	// Convert records to execution info
	executionInfos := make([]*workflowpb.WorkflowExecutionInfo, 0, len(executions))
	for _, record := range executions {
		executionInfo, err := convertToExecutionInfo(record, saTypeMap)
		if err != nil {
			logger.Warn("Failed to convert visibility record", tag.Error(err))
			continue
		}
		executionInfos = append(executionInfos, executionInfo)
	}

	response := &archiver.QueryVisibilityResponse{
		Executions: executionInfos,
	}

	// Set the next page token if there are more results
	if nextMarker != "" {
		nextToken, err := serializeQueryVisibilityToken(&queryVisibilityToken{
			Marker: nextMarker,
		})
		if err != nil {
			logger.Error("Failed to serialize next page token", tag.Error(err))
			return nil, err
		}
		response.NextPageToken = nextToken
	}

	return response, nil
}

// ValidateURI validates the Azure URI
func (v *visibilityArchiver) ValidateURI(URI archiver.URI) error {
	if URI.Scheme() != URIScheme {
		return fmt.Errorf("URI scheme must be %s", URIScheme)
	}
	if URI.Hostname() != v.config.AccountName {
		return fmt.Errorf("URI hostname must match account name: %s", v.config.AccountName)
	}
	if len(URI.Path()) == 0 {
		return fmt.Errorf("URI path cannot be empty")
	}
	return nil
}

// constructVisibilityKey constructs the blob storage key for visibility record
func constructVisibilityKey(
	basePath string,
	namespaceID string,
	workflowTypeName string,
	workflowID string,
	runID string,
	closeTimestamp time.Time,
) string {
	// Structure: basePath/namespaceId/workflowType/YYYY-MM-DD/workflowId_runId.json
	return fmt.Sprintf(
		"%s/%s/%s/%s/%s_%s.json",
		strings.TrimSuffix(basePath, DefaultBlobDelimiter),
		namespaceID,
		workflowTypeName,
		closeTimestamp.Format("2006-01-02"),
		workflowID,
		runID,
	)
}

// constructVisibilitySearchPrefix constructs the search prefix based on the parsed query
func constructVisibilitySearchPrefix(
	basePath string,
	namespaceID string,
	parsedQuery *parsedQuery,
) string {
	prefix := fmt.Sprintf(
		"%s/%s",
		strings.TrimSuffix(basePath, DefaultBlobDelimiter),
		namespaceID,
	)

	if parsedQuery.workflowTypeName != nil {
		prefix = fmt.Sprintf("%s/%s", prefix, *parsedQuery.workflowTypeName)

		if parsedQuery.closeTime != nil {
			prefix = fmt.Sprintf("%s/%s", prefix, formatTimePrefix(*parsedQuery.closeTime, parsedQuery.searchPrecision))
		} else if parsedQuery.startTime != nil {
			prefix = fmt.Sprintf("%s/%s", prefix, formatTimePrefix(*parsedQuery.startTime, parsedQuery.searchPrecision))
		}
	}

	return prefix
}

// formatTimePrefix formats time based on precision
func formatTimePrefix(t time.Time, precision *string) string {
	if precision == nil {
		return t.Format("2006-01-02")
	}

	switch *precision {
	case PrecisionDay:
		return t.Format("2006-01-02")
	case PrecisionHour:
		return t.Format("2006-01-02-15")
	case PrecisionMinute:
		return t.Format("2006-01-02-15-04")
	case PrecisionSecond:
		return t.Format("2006-01-02-15-04-05")
	default:
		return t.Format("2006-01-02")
	}
}

// matchesQuery checks if the visibility record matches the parsed query
func matchesQuery(record *archiverspb.VisibilityRecord, query *parsedQuery) bool {
	if query.workflowID != nil && record.GetWorkflowId() != *query.workflowID {
		return false
	}
	if query.workflowTypeName != nil && record.WorkflowTypeName != *query.workflowTypeName {
		return false
	}
	if query.closeTime != nil {
		closeTime := record.CloseTime.AsTime()
		if !matchesTimeWithPrecision(closeTime, *query.closeTime, query.searchPrecision) {
			return false
		}
	}
	if query.startTime != nil {
		startTime := record.StartTime.AsTime()
		if !matchesTimeWithPrecision(startTime, *query.startTime, query.searchPrecision) {
			return false
		}
	}
	return true
}

// matchesTimeWithPrecision checks if two times match based on precision
func matchesTimeWithPrecision(t1, t2 time.Time, precision *string) bool {
	if precision == nil {
		return t1.Format("2006-01-02") == t2.Format("2006-01-02")
	}

	switch *precision {
	case PrecisionDay:
		return t1.Format("2006-01-02") == t2.Format("2006-01-02")
	case PrecisionHour:
		return t1.Format("2006-01-02-15") == t2.Format("2006-01-02-15")
	case PrecisionMinute:
		return t1.Format("2006-01-02-15-04") == t2.Format("2006-01-02-15-04")
	case PrecisionSecond:
		return t1.Truncate(time.Second).Equal(t2.Truncate(time.Second))
	default:
		return t1.Format("2006-01-02") == t2.Format("2006-01-02")
	}
}

func serializeQueryVisibilityToken(token *queryVisibilityToken) ([]byte, error) {
	return json.Marshal(token)
}

func deserializeQueryVisibilityToken(data []byte) (*queryVisibilityToken, error) {
	var token queryVisibilityToken
	if err := json.Unmarshal(data, &token); err != nil {
		return nil, err
	}
	return &token, nil
}

// convertToExecutionInfo converts a visibility record to workflow execution info
func convertToExecutionInfo(record *archiverspb.VisibilityRecord, saTypeMap searchattribute.NameTypeMap) (*workflowpb.WorkflowExecutionInfo, error) {
	searchAttributes, err := searchattribute.Parse(record.SearchAttributes, &saTypeMap)
	if err != nil {
		return nil, err
	}

	return &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: record.GetWorkflowId(),
			RunId:      record.GetRunId(),
		},
		Type: &commonpb.WorkflowType{
			Name: record.WorkflowTypeName,
		},
		StartTime:         record.StartTime,
		ExecutionTime:     record.ExecutionTime,
		CloseTime:         record.CloseTime,
		ExecutionDuration: record.ExecutionDuration,
		Status:            record.Status,
		HistoryLength:     record.HistoryLength,
		Memo:              record.Memo,
		SearchAttributes:  searchAttributes,
	}, nil
}
