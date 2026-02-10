package catalog

import (
	"context"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/catalog"
)

// GetEffectivePermissions retrieves the effective permissions for a securable.
func GetEffectivePermissions(ctx context.Context, w *databricks.WorkspaceClient, securableType, fullName string) (*catalog.EffectivePermissionsList, error) {
	return w.Grants.GetEffective(ctx, catalog.GetEffectiveRequest{
		SecurableType: securableType,
		FullName:      fullName,
	})
}

// GetPermissions retrieves the direct permissions for a securable.
func GetPermissions(ctx context.Context, w *databricks.WorkspaceClient, securableType, fullName string) (*catalog.GetPermissionsResponse, error) {
	return w.Grants.Get(ctx, catalog.GetGrantRequest{
		SecurableType: securableType,
		FullName:      fullName,
	})
}
