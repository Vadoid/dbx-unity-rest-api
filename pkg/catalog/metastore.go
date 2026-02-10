package catalog

import (
	"context"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/catalog"
)

// GetMetastoreSummary retrieves the summary of the current metastore.
func GetMetastoreSummary(ctx context.Context, w *databricks.WorkspaceClient) (*catalog.GetMetastoreSummaryResponse, error) {
	return w.Metastores.Summary(ctx)
}
