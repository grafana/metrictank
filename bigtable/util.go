package bigtable

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigtable"
	log "github.com/sirupsen/logrus"
)

func EnsureTableExists(ctx context.Context, createCf bool, client *bigtable.AdminClient, tableName string, columnFamilies map[string]bigtable.GCPolicy) error {
	tables, err := client.Tables(ctx)
	if err != nil {
		log.Errorf("bt-ensure-table-exists: failed to list tables: %s", err)
		return err
	}

	found := false
	for _, t := range tables {
		if t == tableName {
			found = true
			break
		}
	}

	if !found {
		if !createCf {
			return fmt.Errorf("table %q does not exist and creation of tables is disabled", tableName)
		}

		log.Infof("bt-ensure-table-exists: table %q does not yet exist. Creating it.", tableName)
		table := bigtable.TableConf{
			TableID:  tableName,
			Families: columnFamilies,
		}

		err := client.CreateTableFromConf(ctx, &table)
		if err != nil {
			log.Errorf("bt-ensure-table-exists: failed to create table %q: %s", tableName, err)
		}

		return err
	}

	log.Infof("bt-ensure-table-exists: table %q exists.", tableName)
	// table exists.  Lets make sure that it has all of the CF's we need.
	table, err := client.TableInfo(ctx, tableName)
	if err != nil {
		log.Errorf("bt-ensure-table-exists: failed to get tableInfo of %q: %s", tableName, err)
		return err
	}

	existingFamilies := make(map[string]string)
	for _, cf := range table.FamilyInfos {
		existingFamilies[cf.Name] = cf.GCPolicy
	}

	for cfName, wantPolicy := range columnFamilies {
		havePolicy, ok := existingFamilies[cfName]
		if !ok {
			log.Infof("bt-ensure-table-exists: column family %s/%s does not exist. Creating it.", tableName, cfName)
			err = client.CreateColumnFamily(ctx, tableName, cfName)
			if err != nil {
				log.Errorf("bt-ensure-table-exists: failed to create cf %s/%s: %s", tableName, cfName, err)
				return err
			}
			err = client.SetGCPolicy(ctx, tableName, cfName, wantPolicy)
			if err != nil {
				log.Errorf("bt-ensure-table-exists: failed to set GCPolicy of %s/%s: %s", tableName, cfName, err)
				return err
			}
		} else if havePolicy == "" {
			log.Infof("bt-ensure-table-exists: column family %s/%s exists but has no GCPolicy. Creating it.", tableName, cfName)
			err = client.SetGCPolicy(ctx, tableName, cfName, wantPolicy)
			if err != nil {
				log.Errorf("bt-ensure-table-exists: failed to set GCPolicy of %s/%s: %s", tableName, cfName, err)
				return err
			}
		}
	}

	return nil
}
