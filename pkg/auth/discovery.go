package auth

import (
	"context"
	"fmt"
	"sort"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/sql"
)

// ListWarehouses retrieves all available SQL Warehouses.
func ListWarehouses(ctx context.Context, w *databricks.WorkspaceClient) ([]sql.EndpointInfo, error) {
	iterator := w.Warehouses.List(ctx, sql.ListWarehousesRequest{})

	var all []sql.EndpointInfo
	for iterator.HasNext(ctx) {
		item, err := iterator.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("error iterating warehouses: %w", err)
		}
		all = append(all, item)
	}
	return all, nil
}

// DiscoverBestWarehouse finds the best SQL Warehouse (RUNNING > STARTING > STOPPED).
func DiscoverBestWarehouse(ctx context.Context, host, token string) (*sql.EndpointInfo, error) {
	config := &databricks.Config{
		Host:  host,
		Token: token,
	}

	w, err := databricks.NewWorkspaceClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create workspace client: %w", err)
	}

	all, err := ListWarehouses(ctx, w)
	if err != nil {
		return nil, err
	}

	if len(all) == 0 {
		return nil, fmt.Errorf("no SQL Warehouses found")
	}

	// Sort by state priority
	// We want descending priority (best first)
	// state: RUNNING, STARTING, STOPPED, etc.
	sorted := make([]sql.EndpointInfo, len(all))
	copy(sorted, all)

	sort.SliceStable(sorted, func(i, j int) bool {
		return getStatePriority(sorted[i].State) < getStatePriority(sorted[j].State)
	})

	return &sorted[0], nil
}

func getStatePriority(state sql.State) int {
	switch state {
	case sql.StateRunning:
		return 0
	case sql.StateStarting:
		return 1
	default:
		return 2
	}
}
