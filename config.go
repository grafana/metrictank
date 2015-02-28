/*
 * Copyright (c) 2015, Raintank Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"github.com/BurntSushi/toml"
	"github.com/ctdk/goas/v2/logger"
	"github.com/jessevdk/go-flags"
	"fmt"
	"errors"
	"log"
	"os"
	"strings"
)

type conf struct {
	RabbitMQURL         string `toml:"rabbitmq-url"`
	GraphiteAddr        string `toml:"graphite-addr"`
	GraphitePort        int    `toml:"graphite-port"`
	ElasticsearchDomain string `toml:"elasticsearch-domain"`
	ElasticsearchPort   int    `toml:"elasticsearch-port"`
	ElasticsearchUser   string `toml:"elasticsearch-user"`
	ElasticsearchPasswd string `toml:"elasticsearch-passwd"`
	RedisAddr           string `toml:"redis-addr"`
	RedisPasswd         string `toml:"redis-passwd"`
	RedisDB             int64  `toml:"redis-db"`
	LogLevel            string `toml:"log-level"`
	LogFile             string `toml:"log-file"`
	SysLog              bool   `toml:"syslog"`
	DebugLevel          int    `toml:"debug-level"`
}

const version = "0.1.0"

type options struct {
	Verbose             []bool `short:"V" long:"verbose" description:"Show verbose debug information. Repeat for more verbosity."`
	Version bool `short:"v" long:"version" description:"Show version information."`
	ConfFile            string `short:"c" long:"config" description:"Specify a configuration file."`
	LogFile             string `short:"L" long:"log-file" description:"Log to file X"`
	SysLog              bool   `short:"s" long:"syslog" description:"Log to syslog rather than a log file. Incompatible with -L/--log-file."`
	GraphiteAddr        string `short:"g" long:"graphite-addr" description:"Graphite IP address or hostname."`
	GraphitePort        int    `short:"p" long:"graphite-port" description:"Port graphite listens on."`
	ElasticsearchDomain string `short:"d" long:"elasticsearch-domain" description:"Elasticseach domain."`
	ElasticsearchPort   int    `short:"t" long:"elasticsearch-port" description:"Port to connect to for Elasticsearch, defaults to 9200."`
	ElasticsearchUser   string `short:"u" long:"elasticsearch-user" description:"Optional username to use when connecting to elasticsearch."`
	ElasticsearchPasswd string `short:"P" long:"elasticsearch-passwd" description:"Optional password to use when connecting to elasticsearch."`
	RedisAddr           string `short:"r" long:"redis-addr" description:"Hostname or IP address of redis server."`
	RedisPasswd         string `short:"y" long:"redis-passwd" description:"Optional password to use when connecting to redis."`
	RedisDB             int64  `short:"D" long:"redis-db" description:"Option database number to use when connecting to redis."`
	RabbitMQURL string `short:"q" long:"rabbitmq-url" description:"RabbitMQ server URL."`
}

var config *conf

var logLevelNames = map[string]int{"debug": 4, "info": 3, "warning": 2, "error": 1, "critical": 0}

func initConfig() {
	config = new(conf)
	err := parseConfig()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func parseConfig() error {
	opts := &options{}
	_, err := flags.Parse(opts)
	if err != nil {
		if err.(*flags.Error).Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			return err
		}
	}

	if opts.Version {
		fmt.Printf("raintank-metric version %s\n", version)
		os.Exit(0)
	}

	if opts.ConfFile != "" {
		if _, err := toml.DecodeFile(opts.ConfFile, config); err != nil {
			return err
		}
	}

	if opts.LogFile != "" {
		config.LogFile = opts.LogFile
	}
	if opts.SysLog {
		config.SysLog = opts.SysLog
	}
	if config.LogFile != "" {
		lfp, lerr := os.Create(config.LogFile)
		if lerr != nil {
			return lerr
		}
		log.SetOutput(lfp)
	}
	if config.LogFile != "" && config.SysLog {
		lerr := errors.New("cannot use both log-file and syslog options at the same time.")
		return lerr
	}
	if dlev := len(opts.Verbose); dlev != 0 {
		config.DebugLevel = dlev
	}
	if config.LogLevel != "" {
		if lev, ok := logLevelNames[strings.ToLower(config.LogLevel)]; ok && config.DebugLevel == 0 {
			config.DebugLevel = lev
		}
	}
	if config.DebugLevel > 4 {
		config.DebugLevel = 4
	}

	config.DebugLevel = int(logger.LevelCritical) - config.DebugLevel
	logger.SetLevel(logger.LogLevel(config.DebugLevel))
	debugLevel := map[int]string{0: "debug", 1: "info", 2: "warning", 3: "error", 4: "critical"}
	log.Printf("Logging at %s level", debugLevel[config.DebugLevel])
	if config.SysLog {
		sl, err := logger.NewSysLogger("raintank-metric")
		if err != nil {
			return err
		}
		logger.SetLogger(sl)
	} else {
		logger.SetLogger(logger.NewGoLogger())
	}
	if opts.GraphiteAddr != "" {
		config.GraphiteAddr = opts.GraphiteAddr
	}
	if opts.GraphitePort != 0 {
		config.GraphitePort = opts.GraphitePort
	}
	if opts.ElasticsearchDomain != "" {
		config.ElasticsearchDomain = opts.ElasticsearchDomain
	}
	if opts.ElasticsearchPort != 0 {
		config.ElasticsearchPort = opts.ElasticsearchPort
	}
	if opts.ElasticsearchUser != "" {
		config.ElasticsearchUser = opts.ElasticsearchUser
	}
	if opts.ElasticsearchPasswd != "" {
		config.ElasticsearchPasswd = opts.ElasticsearchPasswd
	}
	if opts.RedisAddr != "" {
		config.RedisAddr = opts.RedisAddr
	}
	if opts.RedisPasswd != "" {
		config.RedisPasswd = opts.RedisPasswd
	}
	if opts.RedisDB != 0 {
		config.RedisDB = opts.RedisDB
	}

	if config.ElasticsearchPort == 0 {
		config.ElasticsearchPort = 9200
	}
	if config.RabbitMQURL == "" {
		config.RabbitMQURL = "amqp://localhost"
	}
	if config.ElasticsearchDomain == "" {
		config.ElasticsearchDomain = "localhost"
	}
	if config.RedisAddr == "" {
		config.RedisAddr = "localhost"
	}
	logger.Debugf("configuration: %q", config)
	return nil
}
