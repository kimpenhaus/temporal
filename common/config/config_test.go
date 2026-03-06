package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func mustUnmarshal(t *testing.T, src string, dst any) {
	t.Helper()
	require.NoError(t, yaml.Unmarshal([]byte(src), dst))
}

// ---------------------------------------------------------------------------
// AzureArchiver – YAML parsing
// ---------------------------------------------------------------------------

func TestAzureArchiver_ParseYAML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		input            string
		validate         func(t *testing.T, cfg AzureArchiver)
		validateArchival func(t *testing.T, cfg Config)
	}{
		{
			name: "shared key auth",
			input: `
accountName: myaccount
containerName: mycontainer
accountKey: "base64key=="
`,
			validate: func(t *testing.T, cfg AzureArchiver) {
				assert.Equal(t, "myaccount", cfg.AccountName)
				assert.Equal(t, "mycontainer", cfg.ContainerName)
				assert.Equal(t, "base64key==", cfg.AccountKey)
				assert.Empty(t, cfg.SASToken)
				assert.Empty(t, cfg.ManagedIdentityClientID)
				assert.Empty(t, cfg.ConnectionString)
				assert.Empty(t, cfg.Endpoint)
			},
		},
		{
			name: "managed identity auth",
			input: `
accountName: myaccount
containerName: mycontainer
managedIdentityClientID: "00000000-0000-0000-0000-000000000000"
`,
			validate: func(t *testing.T, cfg AzureArchiver) {
				assert.Equal(t, "00000000-0000-0000-0000-000000000000", cfg.ManagedIdentityClientID)
				assert.Empty(t, cfg.AccountKey)
			},
		},
		{
			name: "connection string auth",
			input: `
accountName: myaccount
containerName: mycontainer
connectionString: "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=key==;EndpointSuffix=core.windows.net"
`,
			validate: func(t *testing.T, cfg AzureArchiver) {
				assert.Equal(t, "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=key==;EndpointSuffix=core.windows.net", cfg.ConnectionString)
				assert.Empty(t, cfg.AccountKey)
			},
		},
		{
			name: "sas token auth",
			input: `
accountName: myaccount
containerName: mycontainer
endpoint: "https://myaccount.blob.core.windows.net"
sasToken: "?sv=2021-08-06&sig=xxx"
`,
			validate: func(t *testing.T, cfg AzureArchiver) {
				assert.Equal(t, "?sv=2021-08-06&sig=xxx", cfg.SASToken)
				assert.Equal(t, "https://myaccount.blob.core.windows.net", cfg.Endpoint)
			},
		},
		{
			name: "embedded in archival provider tree",
			input: `
archival:
  history:
    state: "enabled"
    enableRead: true
    provider:
      azure:
        accountName: prodaccount
        containerName: history-archive
        accountKey: "secretkey=="
  visibility:
    state: "enabled"
    enableRead: true
    provider:
      azure:
        accountName: prodaccount
        containerName: history-archive
        accountKey: "secretkey=="
`,
			validateArchival: func(t *testing.T, cfg Config) {
				require.NotNil(t, cfg.Archival)
				require.NotNil(t, cfg.Archival.History)
				require.NotNil(t, cfg.Archival.History.Provider)
				require.NotNil(t, cfg.Archival.History.Provider.Azure)
				require.NotNil(t, cfg.Archival.Visibility)
				require.NotNil(t, cfg.Archival.Visibility.Provider)
				require.NotNil(t, cfg.Archival.Visibility.Provider.Azure)
				assert.Equal(t, "prodaccount", cfg.Archival.History.Provider.Azure.AccountName)
				assert.Equal(t, "history-archive", cfg.Archival.History.Provider.Azure.ContainerName)
				assert.Equal(t, "secretkey==", cfg.Archival.History.Provider.Azure.AccountKey)
				assert.Equal(t, "enabled", cfg.Archival.History.State)
				assert.True(t, cfg.Archival.History.EnableRead)
				assert.Equal(t, "prodaccount", cfg.Archival.Visibility.Provider.Azure.AccountName)
				assert.Equal(t, "history-archive", cfg.Archival.Visibility.Provider.Azure.ContainerName)
				assert.Equal(t, "secretkey==", cfg.Archival.Visibility.Provider.Azure.AccountKey)
				assert.Equal(t, "enabled", cfg.Archival.Visibility.State)
				assert.True(t, cfg.Archival.Visibility.EnableRead)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.validateArchival != nil {
				var cfg Config
				mustUnmarshal(t, tt.input, &cfg)
				tt.validateArchival(t, cfg)
				return
			}

			var cfg AzureArchiver
			mustUnmarshal(t, tt.input, &cfg)
			tt.validate(t, cfg)
		})
	}
}
