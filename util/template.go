package util

import (
	"github.com/pelletier/go-toml"
	log "github.com/sirupsen/logrus"
)

var tomlFiles = make(map[string]*toml.Tree)

func readTomlFile(TomlFilename string) *toml.Tree {
	tree, ok := tomlFiles[TomlFilename]
	if ok {
		return tree
	}
	tree, err := toml.LoadFile(TomlFilename)
	if err != nil {
		log.WithFields(log.Fields{
			"file":  TomlFilename,
			"error": err.Error(),
		}).Fatal("error decoding file")
	}
	tomlFiles[TomlFilename] = tree
	return tree
}

func ReadEntry(TomlFilename string, EntryName string) interface{} {
	tree := readTomlFile(TomlFilename)
	val := tree.Get(EntryName)
	if val == nil {
		log.WithFields(log.Fields{
			"entry": EntryName,
			"file":  TomlFilename,
		}).Fatal("could not find entry in file")
	}
	return val
}
