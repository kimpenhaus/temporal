package azure

import (
	"context"
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"go.temporal.io/server/common/config"
)

func CreateContainerClient(ctx context.Context, config *config.AzureArchiver) (*container.Client, error) {
	serviceURL := GetServiceUrl(config)

	var client *container.Client
	var err error

	switch {
	case config.ConnectionString != "":
		serviceClient, err := azblob.NewClientFromConnectionString(config.ConnectionString, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create client from connection string: %w", err)
		}
		client = serviceClient.ServiceClient().NewContainerClient(config.ContainerName)
	case config.AccountKey != "":
		credential, err := azblob.NewSharedKeyCredential(config.AccountName, config.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create shared key credential: %w", err)
		}
		serviceClient, err := azblob.NewClientWithSharedKeyCredential(serviceURL, credential, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create client with shared key: %w", err)
		}
		client = serviceClient.ServiceClient().NewContainerClient(config.ContainerName)
	case config.SASToken != "":
		serviceClient, err := azblob.NewClientWithNoCredential(fmt.Sprintf("%s?%s", serviceURL, config.SASToken), nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create client from sas token: %w", err)
		}
		client = serviceClient.ServiceClient().NewContainerClient(config.ContainerName)
	case config.ManagedIdentityClientID != "":
		credential, err := azidentity.NewManagedIdentityCredential(&azidentity.ManagedIdentityCredentialOptions{
			ID: azidentity.ClientID(config.ManagedIdentityClientID),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create managed identity credential: %w", err)
		}
		serviceClient, err := azblob.NewClient(serviceURL, credential, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create client with managed identity: %w", err)
		}
		client = serviceClient.ServiceClient().NewContainerClient(config.ContainerName)
	default:
		credential, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create default azure credential: %w", err)
		}
		serviceClient, err := azblob.NewClient(serviceURL, credential, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create client with default credential: %w", err)
		}
		client = serviceClient.ServiceClient().NewContainerClient(config.ContainerName)
	}

	_, err = client.GetProperties(ctx, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if ok := errors.As(err, &respErr); ok && respErr.StatusCode == 404 {
			return nil, fmt.Errorf("container '%s' not found", config.ContainerName)
		}
		return nil, fmt.Errorf("failed to verify container: %w", err)
	}

	return client, nil
}

func GetServiceUrl(config *config.AzureArchiver) string {
	if config.Endpoint != "" {
		return config.Endpoint
	}

	return fmt.Sprintf("https://%s.blob.core.windows.net/", config.AccountName)
}
