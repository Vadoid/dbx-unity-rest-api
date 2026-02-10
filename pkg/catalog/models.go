package catalog

import (
	"context"
	"fmt"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/catalog"
)

// ListModels retrieves all registered models in a specific schema.
func ListModels(ctx context.Context, w *databricks.WorkspaceClient, catalogName, schemaName string) ([]catalog.RegisteredModelInfo, error) {
	request := catalog.ListRegisteredModelsRequest{
		CatalogName: catalogName,
		SchemaName:  schemaName,
	}

	it := w.RegisteredModels.List(ctx, request)
	var all []catalog.RegisteredModelInfo
	for it.HasNext(ctx) {
		m, err := it.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to iterate models: %w", err)
		}
		all = append(all, m)
	}
	return all, nil
}

// GetModel retrieves details for a specific registered model.
func GetModel(ctx context.Context, w *databricks.WorkspaceClient, fullName string) (*catalog.RegisteredModelInfo, error) {
	return w.RegisteredModels.Get(ctx, catalog.GetRegisteredModelRequest{FullName: fullName})
}
