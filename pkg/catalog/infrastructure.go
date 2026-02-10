package catalog

import (
	"context"
	"fmt"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/catalog"
)

// ListConnections retrieves all Lakehouse Federation connections.
func ListConnections(ctx context.Context, w *databricks.WorkspaceClient) ([]catalog.ConnectionInfo, error) {
	it := w.Connections.List(ctx, catalog.ListConnectionsRequest{})
	var all []catalog.ConnectionInfo
	for it.HasNext(ctx) {
		c, err := it.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to iterate connections: %w", err)
		}
		all = append(all, c)
	}
	return all, nil
}

// ListExternalLocations retrieves all external locations.
func ListExternalLocations(ctx context.Context, w *databricks.WorkspaceClient) ([]catalog.ExternalLocationInfo, error) {
	it := w.ExternalLocations.List(ctx, catalog.ListExternalLocationsRequest{})
	var all []catalog.ExternalLocationInfo
	for it.HasNext(ctx) {
		el, err := it.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to iterate external locations: %w", err)
		}
		all = append(all, el)
	}
	return all, nil
}

// ListStorageCredentials retrieves all storage credentials.
func ListStorageCredentials(ctx context.Context, w *databricks.WorkspaceClient) ([]catalog.StorageCredentialInfo, error) {
	it := w.StorageCredentials.List(ctx, catalog.ListStorageCredentialsRequest{})
	var all []catalog.StorageCredentialInfo
	for it.HasNext(ctx) {
		sc, err := it.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to iterate storage credentials: %w", err)
		}
		all = append(all, sc)
	}
	return all, nil
}
