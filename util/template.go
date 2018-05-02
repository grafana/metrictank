package util

import (
	"github.com/pelletier/go-toml"
	"log"
)

var tomlFiles = make(map[string]*toml.Tree)

func readTomlFile(TomlFilename string) *toml.Tree {
	tree, ok := tomlFiles[TomlFilename]
	if ok {
		return tree
	}
	tree, err := toml.LoadFile(TomlFilename)
	if err != nil {
		log.Fatalf("Error decoding file %q:\n%s\n", TomlFilename, err)
	}
	tomlFiles[TomlFilename] = tree
	return tree
}

func ReadEntry(TomlFilename string, EntryName string) interface{} {
	tree := readTomlFile(TomlFilename)
	val := tree.Get(EntryName)
	if val == nil {
		log.Fatalf("Error %q does not exist in %q", EntryName, TomlFilename)
	}
	return val
}
