package catalog

import (
	"context"
	"fmt"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/catalog"
)

// ListVolumes retrieves all volumes in a specific schema.
func ListVolumes(ctx context.Context, w *databricks.WorkspaceClient, catalogName, schemaName string) ([]catalog.VolumeInfo, error) {
	request := catalog.ListVolumesRequest{
		CatalogName: catalogName,
		SchemaName:  schemaName,
	}

	it := w.Volumes.List(ctx, request)
	var all []catalog.VolumeInfo
	for it.HasNext(ctx) {
		v, err := it.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to iterate volumes: %w", err)
		}
		all = append(all, v)
	}
	return all, nil
}

// GetVolume retrieves details for a specific volume.
func GetVolume(ctx context.Context, w *databricks.WorkspaceClient, fullName string) (*catalog.VolumeInfo, error) {
	return w.Volumes.Read(ctx, catalog.ReadVolumeRequest{Name: fullName})
}
