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

package setting

import (
	"errors"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/ctdk/goas/v2/logger"
	"github.com/jessevdk/go-flags"
	"log"
	"os"
	"strings"
)

type Conf struct {
	RabbitMQURL         string `toml:"rabbitmq-url"`
	CarbonAddr          string `toml:"carbon-addr"`
	CarbonPort          int    `toml:"carbon-port"`
	EnableCarbon        bool   `toml:"enable-carbon"`
	KairosdbUrl         string `toml:"kairosdb-url"`
	EnableKairosdb      bool   `toml:"enable-kairosdb"`
	ElasticsearchDomain string `toml:"elasticsearch-domain"`
	ElasticsearchPort   int    `toml:"elasticsearch-port"`
	ElasticsearchUser   string `toml:"elasticsearch-user"`
	ElasticsearchPasswd string `toml:"elasticsearch-passwd"`
	ExpvarAddr          string `toml:"expvar-addr"`
	RedisAddr           string `toml:"redis-addr"`
	RedisPasswd         string `toml:"redis-passwd"`
	RedisDB             int64  `toml:"redis-db"`
	LogLevel            string `toml:"log-level"`
	LogFile             string `toml:"log-file"`
	SysLog              bool   `toml:"syslog"`
	DebugLevel          int    `toml:"debug-level"`
	NumWorkers          int    `toml:"num-workers"`
}

const version = "0.1.0"

type options struct {
	Verbose             []bool `short:"V" long:"verbose" description:"Show verbose debug information. Repeat for more verbosity."`
	Version             bool   `short:"v" long:"version" description:"Show version information."`
	ConfFile            string `short:"c" long:"config" description:"Specify a configuration file."`
	LogFile             string `short:"L" long:"log-file" description:"Log to file X"`
	SysLog              bool   `short:"s" long:"syslog" description:"Log to syslog rather than a log file. Incompatible with -L/--log-file."`
	EnableCarbon        bool   `short:"C" long:"enable-carbon" description:"Enable writing metrics to carbon server."`
	CarbonAddr          string `short:"g" long:"carbon-addr" description:"Carbon IP address or hostname."`
	CarbonPort          int    `short:"p" long:"carbon-port" description:"Port Carbon server listens on."`
	KairosdbUrl         string `short:"k" long:"kairosdb-url" description:"KairosDB url"`
	EnableKairosdb      bool   `short:"K" long:"enable-kairosdb" description:"Enable writing metrics to Kairosdb."`
	ElasticsearchDomain string `short:"d" long:"elasticsearch-domain" description:"Elasticseach domain."`
	ElasticsearchPort   int    `short:"t" long:"elasticsearch-port" description:"Port to connect to for Elasticsearch, defaults to 9200."`
	ElasticsearchUser   string `short:"u" long:"elasticsearch-user" description:"Optional username to use when connecting to elasticsearch."`
	ElasticsearchPasswd string `short:"P" long:"elasticsearch-passwd" description:"Optional password to use when connecting to elasticsearch."`
	RedisAddr           string `short:"r" long:"redis-addr" description:"Hostname or IP address of redis server."`
	RedisPasswd         string `short:"y" long:"redis-passwd" description:"Optional password to use when connecting to redis."`
	RedisDB             int64  `short:"D" long:"redis-db" description:"Option database number to use when connecting to redis."`
	RabbitMQURL         string `short:"q" long:"rabbitmq-url" description:"RabbitMQ server URL."`
	NumWorkers          int    `short:"w" long:"num-workers" description:"Number of workers to launch. Defaults to the number of CPUs on the system."`
	ExpvarAddr          string `short:"e" long:"expvar-addr" description:"address to expose expvars on."`
}

var Config *Conf

var logLevelNames = map[string]int{"debug": 4, "info": 3, "warning": 2, "error": 1, "critical": 0}

func InitConfig() {
	Config = new(Conf)
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

	if opts.ConfFile == "" {
		opts.ConfFile = "/etc/raintank-metric/raintank.conf"
	}
	if _, err := toml.DecodeFile(opts.ConfFile, Config); err != nil {
		return err
	}

	if opts.LogFile != "" {
		Config.LogFile = opts.LogFile
	}
	if opts.SysLog {
		Config.SysLog = opts.SysLog
	}
	if Config.LogFile != "" {
		lfp, lerr := os.Create(Config.LogFile)
		if lerr != nil {
			return lerr
		}
		log.SetOutput(lfp)
	}
	if Config.LogFile != "" && Config.SysLog {
		lerr := errors.New("cannot use both log-file and syslog options at the same time.")
		return lerr
	}
	if dlev := len(opts.Verbose); dlev != 0 {
		Config.DebugLevel = dlev
	}
	if Config.LogLevel != "" {
		if lev, ok := logLevelNames[strings.ToLower(Config.LogLevel)]; ok && Config.DebugLevel == 0 {
			Config.DebugLevel = lev
		}
	}
	if Config.DebugLevel > 4 {
		Config.DebugLevel = 4
	}

	Config.DebugLevel = int(logger.LevelCritical) - Config.DebugLevel
	logger.SetLevel(logger.LogLevel(Config.DebugLevel))
	debugLevel := map[int]string{0: "debug", 1: "info", 2: "warning", 3: "error", 4: "critical"}
	log.Printf("Logging at %s level", debugLevel[Config.DebugLevel])
	if Config.SysLog {
		sl, err := logger.NewSysLogger("raintank-metric")
		if err != nil {
			return err
		}
		logger.SetLogger(sl)
	} else {
		logger.SetLogger(logger.NewGoLogger())
	}
	if opts.EnableCarbon {
		Config.EnableCarbon = true
	}
	if opts.CarbonAddr != "" {
		Config.CarbonAddr = opts.CarbonAddr
	}
	if opts.CarbonPort != 0 {
		Config.CarbonPort = opts.CarbonPort
	}
	if opts.EnableKairosdb {
		Config.EnableKairosdb = true
	}
	if opts.KairosdbUrl != "" {
		Config.KairosdbUrl = opts.KairosdbUrl
	}
	if opts.ElasticsearchDomain != "" {
		Config.ElasticsearchDomain = opts.ElasticsearchDomain
	}
	if opts.ElasticsearchPort != 0 {
		Config.ElasticsearchPort = opts.ElasticsearchPort
	}
	if opts.ElasticsearchUser != "" {
		Config.ElasticsearchUser = opts.ElasticsearchUser
	}
	if opts.ElasticsearchPasswd != "" {
		Config.ElasticsearchPasswd = opts.ElasticsearchPasswd
	}
	if opts.RedisAddr != "" {
		Config.RedisAddr = opts.RedisAddr
	}
	if opts.RedisPasswd != "" {
		Config.RedisPasswd = opts.RedisPasswd
	}
	if opts.RedisDB != 0 {
		Config.RedisDB = opts.RedisDB
	}
	if opts.ExpvarAddr != "" {
		Config.ExpvarAddr = opts.ExpvarAddr
	}
	if opts.NumWorkers != 0 {
		Config.NumWorkers = opts.NumWorkers
	}
	if Config.NumWorkers < 0 {
		return errors.New("--num-workers must be a number greater than zero")
	}

	if Config.ElasticsearchPort == 0 {
		Config.ElasticsearchPort = 9200
	}
	if Config.RabbitMQURL == "" {
		Config.RabbitMQURL = "amqp://localhost"
	}
	if Config.ElasticsearchDomain == "" {
		Config.ElasticsearchDomain = "localhost"
	}
	if Config.RedisAddr == "" {
		Config.RedisAddr = "localhost"
	}

	logger.Debugf("Configuration: %q", Config)
	return nil
}
