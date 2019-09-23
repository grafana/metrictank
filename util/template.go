package util

import (
	"io"
	"log"
	"os"

	"github.com/pelletier/go-toml"
)

func ReadAllEntries(fileReader io.Reader) (map[string]string, error) {
	tree, err := toml.LoadReader(fileReader)
	if err != nil {
		return nil, err
	}

	res := make(map[string]string)
	for _, key := range tree.Keys() {
		res[key] = tree.Get(key).(string)
	}

	return res, nil
}

func ReadEntry(schemaFile, entry string) string {
	schemaFileReader, err := os.Open(schemaFile)
	if err != nil {
		log.Fatalf("Failed to open schema file %s: %s", schemaFile, err)
	}
	defer schemaFileReader.Close()

	tableSchemas, err := ReadAllEntries(schemaFileReader)
	if err != nil {
		log.Fatalf("Failed to read schemas from file %s: %s", schemaFile, err)
	}

	schemaTable, ok := tableSchemas["schema_table"]
	if !ok {
		log.Fatalf("Table schemas are missing section \"schema_table\"")
	}

	return schemaTable
}
