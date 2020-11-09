package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigtable"
	btUtils "github.com/grafana/metrictank/bigtable"
)

const COLUMN_FAMILY = "idx"

type btClient struct {
	client *bigtable.Client
	tbl    *bigtable.Table
}

func NewBtClient(project, instance, tableName string) (*btClient, error) {
	ctx := context.Background()
	adminClient, err := bigtable.NewAdminClient(ctx, project, instance)
	if err != nil {
		return nil, fmt.Errorf("failed to create bigtable admin client: %s", err)
	}
	err = btUtils.EnsureTableExists(ctx, false, adminClient, tableName, map[string]bigtable.GCPolicy{
		COLUMN_FAMILY: bigtable.MaxVersionsPolicy(1),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to ensure that table %q exists: %s", tableName, err)
	}

	client, err := bigtable.NewClient(ctx, project, instance)
	if err != nil {
		return nil, fmt.Errorf("failed to create bigtable client: %s", err)
	}

	return &btClient{
		client,
		client.Open(tableName),
	}, nil
}
