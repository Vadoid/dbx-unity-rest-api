package catalog

import (
	"context"
	"fmt"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/catalog"
)

// ListFunctions retrieves all functions in a specific schema.
func ListFunctions(ctx context.Context, w *databricks.WorkspaceClient, catalogName, schemaName string) ([]catalog.FunctionInfo, error) {
	request := catalog.ListFunctionsRequest{
		CatalogName: catalogName,
		SchemaName:  schemaName,
	}
	// We use manual iteration as All() is not available
	it := w.Functions.List(ctx, request)
	var all []catalog.FunctionInfo
	for it.HasNext(ctx) {
		f, err := it.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to iterate functions: %w", err)
		}
		all = append(all, f)
	}
	return all, nil
}

// GetFunction retrieves details for a specific function.
func GetFunction(ctx context.Context, w *databricks.WorkspaceClient, name string) (*catalog.FunctionInfo, error) {
	return w.Functions.Get(ctx, catalog.GetFunctionRequest{Name: name})
}
