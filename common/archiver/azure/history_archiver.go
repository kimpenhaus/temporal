package azure

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

const (
	// URIScheme is the scheme for Azure URIs
	URIScheme = "azblob"
	// DefaultBlobDelimiter is the default delimiter used in blob storage paths
	DefaultBlobDelimiter = "/"

	errEncodeHistory      = "failed to encode history batches"
	errWriteBlob          = "failed to write history to blob storage"
	targetHistoryBlobSize = 2 * 1024 * 1024 // 2MB
)

type (
	historyArchiver struct {
		containerClient  *container.Client
		config           *config.AzureArchiver
		metricsHandler   metrics.Handler
		logger           log.Logger
		executionManager persistence.ExecutionManager
		// only set in test code
		historyIterator archiver.HistoryIterator
	}

	getHistoryToken struct {
		CloseFailoverVersion int64
		BatchIdx             int
	}

	uploadProgress struct {
		BatchIdx      int
		IteratorState []byte
		uploadedSize  int64
		historySize   int64
	}
)

// NewHistoryArchiver creates a new Azure blob history archiver
func NewHistoryArchiver(
	executionManager persistence.ExecutionManager,
	logger log.Logger,
	metricsHandler metrics.Handler,
	config *config.AzureArchiver,
) (archiver.HistoryArchiver, error) {
	containerClient, err := CreateContainerClient(context.TODO(), config)
	if err != nil {
		return nil, err
	}

	return &historyArchiver{
		containerClient:  containerClient,
		config:           config,
		executionManager: executionManager,
		metricsHandler:   metricsHandler,
		logger:           logger,
	}, nil
}

// Archive archives the workflow history to Azure blob storage
func (h *historyArchiver) Archive(
	ctx context.Context,
	URI archiver.URI,
	request *archiver.ArchiveHistoryRequest,
	opts ...archiver.ArchiveOption,
) (err error) {
	handler := h.metricsHandler.WithTags(metrics.OperationTag(metrics.HistoryArchiverScope), metrics.NamespaceTag(request.Namespace))
	featureCatalog := archiver.GetFeatureCatalog(opts...)
	startTime := time.Now().UTC()

	defer func() {
		metrics.ServiceLatency.With(handler).Record(time.Since(startTime))
		if err != nil {
			if common.IsPersistenceTransientError(err) || isRetryableError(err) {
				metrics.HistoryArchiverArchiveTransientErrorCount.With(handler).Record(1)
			} else {
				metrics.HistoryArchiverArchiveNonRetryableErrorCount.With(handler).Record(1)
				if featureCatalog.NonRetryableError != nil {
					err = featureCatalog.NonRetryableError()
				}
			}
		}
	}()

	logger := archiver.TagLoggerWithArchiveHistoryRequestAndURI(h.logger, request, URI.String())

	if err := h.ValidateURI(URI); err != nil {
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidURI), tag.Error(err))
		return err
	}

	if err := archiver.ValidateHistoryArchiveRequest(request); err != nil {
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidArchiveRequest), tag.Error(err))
		return err
	}

	var progress uploadProgress
	historyIterator := h.historyIterator
	if historyIterator == nil { // will only be set by testing code
		historyIterator = loadHistoryIterator(ctx, request, h.executionManager, featureCatalog, &progress)
	}

	for historyIterator.HasNext() {
		historyBlob, err := historyIterator.Next(ctx)
		if err != nil {
			var notFound *serviceerror.NotFound
			if errors.As(err, &notFound) {
				// workflow history no longer exists, may due to duplicated archival signal
				logger.Info(archiver.ArchiveSkippedInfoMsg)
				metrics.HistoryArchiverDuplicateArchivalsCount.With(handler).Record(1)
				return nil
			}

			logger := log.With(logger, tag.ArchivalArchiveFailReason(archiver.ErrReasonReadHistory), tag.Error(err))
			if common.IsPersistenceTransientError(err) {
				logger.Error(archiver.ArchiveTransientErrorMsg)
			} else {
				logger.Error(archiver.ArchiveNonRetryableErrorMsg)
			}
			return err
		}

		if historyMutated(request, historyBlob.Body, historyBlob.Header.IsLast) {
			logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonHistoryMutated))
			return archiver.ErrHistoryMutated
		}

		encoder := codec.NewJSONPBEncoder()
		encodedHistoryBlob, err := encoder.Encode(historyBlob)
		if err != nil {
			logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(errEncodeHistory), tag.Error(err))
			return err
		}

		key := constructHistoryKey(URI.Path(), request.NamespaceID, request.WorkflowID, request.RunID, request.CloseFailoverVersion, progress.BatchIdx)

		exists, err := blobExists(ctx, h.containerClient, key)
		if err != nil {
			if isRetryableError(err) {
				logger.Error(archiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason(errWriteBlob), tag.Error(err))
			} else {
				logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(errWriteBlob), tag.Error(err))
			}
			return err
		}

		blobSize := int64(binary.Size(encodedHistoryBlob))
		if exists {
			metrics.HistoryArchiverBlobExistsCount.With(handler).Record(1)
		} else {
			blobClient := h.containerClient.NewBlockBlobClient(key)
			_, err := blobClient.UploadBuffer(ctx, encodedHistoryBlob, &azblob.UploadBufferOptions{
				BlockSize:   4 * 1024 * 1024,
				Concurrency: 3,
			})
			if err != nil {
				if isRetryableError(err) {
					logger.Error(archiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason(errWriteBlob), tag.Error(err))
				} else {
					logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(errWriteBlob), tag.Error(err))
				}
				return err
			}
			progress.uploadedSize += blobSize
			handler.Histogram(metrics.HistoryArchiverBlobSize.Name(), metrics.HistoryArchiverBlobSize.Unit()).Record(blobSize)
		}

		progress.historySize += blobSize
		progress.BatchIdx = progress.BatchIdx + 1
		saveHistoryIteratorState(ctx, featureCatalog, historyIterator, &progress)
	}

	handler.Histogram(metrics.HistoryArchiverTotalUploadSize.Name(), metrics.HistoryArchiverTotalUploadSize.Unit()).Record(progress.uploadedSize)
	handler.Histogram(metrics.HistoryArchiverHistorySize.Name(), metrics.HistoryArchiverHistorySize.Unit()).Record(progress.historySize)
	metrics.HistoryArchiverArchiveSuccessCount.With(handler).Record(1)
	return nil
}

// Get retrieves the archived workflow history from Azure blob storage
func (h *historyArchiver) Get(
	ctx context.Context,
	URI archiver.URI,
	request *archiver.GetHistoryRequest,
) (*archiver.GetHistoryResponse, error) {
	if err := h.ValidateURI(URI); err != nil {
		return nil, serviceerror.NewInvalidArgument(archiver.ErrInvalidURI.Error())
	}

	if err := archiver.ValidateGetRequest(request); err != nil {
		return nil, serviceerror.NewInvalidArgument(archiver.ErrInvalidGetHistoryRequest.Error())
	}

	var err error
	var token *getHistoryToken
	if request.NextPageToken != nil {
		token, err = deserializeGetHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument(archiver.ErrNextPageTokenCorrupted.Error())
		}
	} else if request.CloseFailoverVersion != nil {
		token = &getHistoryToken{
			CloseFailoverVersion: *request.CloseFailoverVersion,
		}
	} else {
		highestVersion, err := h.getHighestVersion(ctx, URI, request)
		if err != nil {
			if err == archiver.ErrHistoryNotExist {
				return nil, serviceerror.NewNotFound(err.Error())
			}
			return nil, serviceerror.NewInvalidArgument(err.Error())
		}
		token = &getHistoryToken{
			CloseFailoverVersion: *highestVersion,
		}
	}

	encoder := codec.NewJSONPBEncoder()
	response := &archiver.GetHistoryResponse{}
	numOfEvents := 0
	isTruncated := false

	for {
		if numOfEvents >= request.PageSize {
			isTruncated = true
			break
		}

		key := constructHistoryKey(URI.Path(), request.NamespaceID, request.WorkflowID, request.RunID, token.CloseFailoverVersion, token.BatchIdx)

		blobClient := h.containerClient.NewBlockBlobClient(key)
		downloadResp, err := blobClient.DownloadStream(ctx, &blob.DownloadStreamOptions{})
		if err != nil {
			if isRetryableError(err) {
				return nil, serviceerror.NewUnavailable(err.Error())
			}
			return nil, convertAzureError(err)
		}

		encodedRecord, err := io.ReadAll(downloadResp.Body)
		closeErr := downloadResp.Body.Close()
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}
		if closeErr != nil {
			return nil, serviceerror.NewInternal(closeErr.Error())
		}

		historyBlob := archiverspb.HistoryBlob{}
		err = encoder.Decode(encodedRecord, &historyBlob)
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}

		for _, batch := range historyBlob.Body {
			response.HistoryBatches = append(response.HistoryBatches, batch)
			numOfEvents += len(batch.Events)
		}

		if historyBlob.Header.IsLast {
			break
		}
		token.BatchIdx++
	}

	if isTruncated {
		nextToken, err := serializeGetHistoryToken(token)
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}
		response.NextPageToken = nextToken
	}

	return response, nil
}

// ValidateURI validates the Azure URI
func (h *historyArchiver) ValidateURI(URI archiver.URI) error {
	if URI.Scheme() != URIScheme {
		return fmt.Errorf("URI scheme must be %s", URIScheme)
	}
	if URI.Hostname() != h.config.AccountName {
		return fmt.Errorf("URI hostname must match account name: %s", h.config.AccountName)
	}
	if len(URI.Path()) == 0 {
		return fmt.Errorf("URI path cannot be empty")
	}
	return nil
}

// constructHistoryKey constructs the blob storage key for history
func constructHistoryKey(basePath, namespaceID, workflowID, runID string, version int64, batchIdx int) string {
	return fmt.Sprintf(
		"%s/%s/%s/%s/%d/%d",
		strings.TrimSuffix(basePath, DefaultBlobDelimiter),
		namespaceID,
		workflowID,
		runID,
		version,
		batchIdx,
	)
}

func constructHistoryKeyPrefix(basePath, namespaceID, workflowID, runID string) string {
	return fmt.Sprintf(
		"%s/%s/%s/%s",
		strings.TrimSuffix(basePath, DefaultBlobDelimiter),
		namespaceID,
		workflowID,
		runID,
	)
}

func serializeGetHistoryToken(token *getHistoryToken) ([]byte, error) {
	data, err := json.Marshal(token)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func deserializeGetHistoryToken(data []byte) (*getHistoryToken, error) {
	var token getHistoryToken
	if err := json.Unmarshal(data, &token); err != nil {
		return nil, err
	}
	return &token, nil
}

// blobExists checks if a blob exists in Azure storage
func blobExists(ctx context.Context, containerClient *container.Client, key string) (bool, error) {
	blobClient := containerClient.NewBlobClient(key)
	_, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == 404 {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// convertAzureError converts Azure SDK errors to archiver errors
func convertAzureError(err error) error {
	if err == nil {
		return nil
	}

	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		if respErr.StatusCode == 404 {
			return serviceerror.NewNotFound("workflow history not found")
		}
		if respErr.StatusCode >= 400 && respErr.StatusCode < 500 {
			return serviceerror.NewInvalidArgument(err.Error())
		}
		if respErr.StatusCode >= 500 {
			return serviceerror.NewUnavailable(err.Error())
		}
	}

	return err
}

// isRetryableError checks if an error is retryable
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		// Retry on server errors (5xx) and throttling (429)
		return respErr.StatusCode >= 500 || respErr.StatusCode == 429
	}
	return false
}

func historyMutated(request *archiver.ArchiveHistoryRequest, historyBatches []*historypb.History, isLast bool) bool {
	lastBatch := historyBatches[len(historyBatches)-1].Events
	lastEvent := lastBatch[len(lastBatch)-1]
	lastFailoverVersion := lastEvent.GetVersion()
	if lastFailoverVersion > request.CloseFailoverVersion {
		return true
	}

	if !isLast {
		return false
	}
	lastEventID := lastEvent.GetEventId()
	return lastFailoverVersion != request.CloseFailoverVersion || lastEventID+1 != request.NextEventID
}

func (h *historyArchiver) getHighestVersion(ctx context.Context, URI archiver.URI, request *archiver.GetHistoryRequest) (*int64, error) {
	prefix := constructHistoryKeyPrefix(URI.Path(), request.NamespaceID, request.WorkflowID, request.RunID) + "/"

	pager := h.containerClient.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{
		Prefix: &prefix,
	})

	var highestVersion *int64

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			var respErr *azcore.ResponseError
			if errors.As(err, &respErr) && respErr.StatusCode == 404 {
				return nil, serviceerror.NewInvalidArgument("container not found")
			}
			return nil, err
		}

		for _, prefix := range page.Segment.BlobPrefixes {
			if prefix.Name == nil {
				continue
			}
			// Extract version from prefix
			parts := strings.Split(strings.Trim(*prefix.Name, "/"), "/")
			if len(parts) == 0 {
				continue
			}
			versionStr := parts[len(parts)-1]
			var version int64
			if _, err := fmt.Sscanf(versionStr, "%d", &version); err != nil {
				continue
			}
			if highestVersion == nil || version > *highestVersion {
				highestVersion = &version
			}
		}
	}

	if highestVersion == nil {
		return nil, archiver.ErrHistoryNotExist
	}
	return highestVersion, nil
}

func loadHistoryIterator(ctx context.Context, request *archiver.ArchiveHistoryRequest, executionManager persistence.ExecutionManager, featureCatalog *archiver.ArchiveFeatureCatalog, progress *uploadProgress) archiver.HistoryIterator {
	if featureCatalog.ProgressManager != nil {
		if featureCatalog.ProgressManager.HasProgress(ctx) {
			err := featureCatalog.ProgressManager.LoadProgress(ctx, progress)
			if err == nil {
				historyIterator, err := archiver.NewHistoryIteratorFromState(request, executionManager, targetHistoryBlobSize, progress.IteratorState)
				if err == nil {
					return historyIterator
				}
			}
			progress.IteratorState = nil
			progress.BatchIdx = 0
			progress.historySize = 0
			progress.uploadedSize = 0
		}
	}
	return archiver.NewHistoryIterator(request, executionManager, targetHistoryBlobSize)
}

func saveHistoryIteratorState(ctx context.Context, featureCatalog *archiver.ArchiveFeatureCatalog, historyIterator archiver.HistoryIterator, progress *uploadProgress) {
	// Saving history state is a best effort operation. Ignore errors and continue
	if featureCatalog.ProgressManager != nil {
		state, err := historyIterator.GetState()
		if err != nil {
			return
		}
		progress.IteratorState = state
		_ = featureCatalog.ProgressManager.RecordProgress(ctx, progress)
	}
}
