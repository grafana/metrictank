module github.com/grafana/metrictank

go 1.19

require (
	cloud.google.com/go/bigtable v1.16.0
	github.com/Dieterbe/artisanalhistogram v0.0.0-20210330070510-f7596a8a7dbe
	github.com/Dieterbe/profiletrigger v0.0.0-20201002131902-3fc6b4ea613c
	github.com/Shopify/sarama v1.26.1
	github.com/alyu/configparser v0.0.0-20151125021232-26b2fe18bee1
	github.com/cespare/xxhash v1.1.0
	github.com/davecgh/go-spew v1.1.1
	github.com/dgryski/go-jump v0.0.0-20170409065014-e1f439676b57
	github.com/dgryski/go-linlog v0.0.0-20210326150603-ef0e282fbe89
	github.com/docker/docker v1.13.1
	github.com/go-macaron/binding v1.0.0
	github.com/gocql/gocql v0.0.0-20191126110522-1982a06ad6b9
	github.com/golang/snappy v0.0.3
	github.com/google/go-cmp v0.5.8
	github.com/gosuri/uilive v0.0.4
	github.com/grafana/configparser v0.0.0-20210707122942-2593eb86a3ee
	github.com/grafana/globalconf v0.0.0-20181214112547-7a1aae0695d9
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed
	github.com/hashicorp/golang-lru v0.5.4
	github.com/hashicorp/memberlist v0.4.0
	github.com/jpillora/backoff v0.0.0-20170918002102-8eab2debe79d
	github.com/kisielk/og-rek v1.0.0
	github.com/kisielk/whisper-go v0.0.0-20140112135752-82e8091afdea
	github.com/metrics20/go-metrics20 v0.0.0-20171130201432-27c134d83f76
	github.com/mitchellh/go-homedir v1.1.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pelletier/go-toml v1.9.5
	github.com/prometheus/client_golang v1.4.0
	github.com/prometheus/procfs v0.0.8
	github.com/raintank/dur v0.0.0-20181019115741-955e3a77c6a8
	github.com/raintank/gziper v0.0.0-20180718203907-25a564af7e47
	github.com/raintank/met v0.0.0-20161103102816-05a94bb32ad1
	github.com/raintank/worldping-api v0.0.0-20170608095801-055842d60eb4
	github.com/rs/cors v0.0.0-20160617231935-a62a804a8a00
	github.com/sergi/go-diff v1.0.0
	github.com/sirupsen/logrus v1.4.2
	github.com/smartystreets/goconvey v1.6.4
	github.com/spenczar/tdigest v2.1.0+incompatible
	github.com/spf13/cobra v0.0.5
	github.com/spf13/viper v1.6.2
	github.com/tinylib/msgp v1.1.0
	github.com/uber/jaeger-client-go v2.30.0+incompatible
	github.com/xdg/scram v0.0.1
	golang.org/x/sync v0.0.0-20220929204114-8fcdb60fdcc0
	golang.org/x/time v0.0.0-20200630173020-3af7569d3a1e
	gopkg.in/macaron.v1 v1.3.4
)

require (
	cloud.google.com/go v0.102.1 // indirect
	cloud.google.com/go/compute v1.7.0 // indirect
	cloud.google.com/go/iam v0.3.0 // indirect
	github.com/DataDog/datadog-go v3.2.0+incompatible // indirect
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/Microsoft/go-winio v0.4.5 // indirect
	github.com/alexcesaro/statsd v2.0.0+incompatible // indirect
	github.com/armon/go-metrics v0.3.10 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/census-instrumentation/opencensus-proto v0.2.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/cncf/udpa/go v0.0.0-20210930031921-04548b0d99d4 // indirect
	github.com/cncf/xds/go v0.0.0-20211011173535-cb28da3451f1 // indirect
	github.com/docker/distribution v2.6.2+incompatible // indirect
	github.com/docker/go-connections v0.3.0 // indirect
	github.com/docker/go-units v0.3.2 // indirect
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/envoyproxy/go-control-plane v0.10.2-0.20220325020618-49ff273808a1 // indirect
	github.com/envoyproxy/protoc-gen-validate v0.1.0 // indirect
	github.com/frankban/quicktest v1.11.0 // indirect
	github.com/fsnotify/fsnotify v1.5.1 // indirect
	github.com/glacjay/goini v0.0.0-20161120062552-fd3024d87ee2 // indirect
	github.com/go-macaron/inject v0.0.0-20160627170012-d8a0b8677191 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/uuid v1.1.2 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.1.0 // indirect
	github.com/googleapis/gax-go/v2 v2.4.0 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20190430165422-3e4dfb77656c // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-sockaddr v1.0.2 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/klauspost/compress v1.10.2 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/magiconair/properties v1.8.1 // indirect
	github.com/mattn/go-isatty v0.0.12 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/miekg/dns v1.1.41 // indirect
	github.com/mitchellh/mapstructure v1.4.1 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pierrec/lz4 v2.5.2+incompatible // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.9.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20190826022208-cac0b30c2563 // indirect
	github.com/rs/xhandler v0.0.0-20160618193221-ed27b6fd6521 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/smartystreets/assertions v1.0.1 // indirect
	github.com/spf13/afero v1.2.2 // indirect
	github.com/spf13/cast v1.3.1 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stevvooe/resumable v0.0.0-20180830230917-22b14a53ba50 // indirect
	github.com/subosito/gotenv v1.2.0 // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/unknwon/com v0.0.0-20190804042917-757f69c95f3e // indirect
	github.com/xdg/stringprep v1.0.0 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.uber.org/atomic v1.4.0 // indirect
	golang.org/x/crypto v0.0.0-20210513164829-c07d793c2f9a // indirect
	golang.org/x/net v0.0.0-20220617184016-355a448f1bc9 // indirect
	golang.org/x/oauth2 v0.0.0-20220608161450-d0670ef3b1eb // indirect
	golang.org/x/sys v0.0.0-20221006211917-84dc82d7e875 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/xerrors v0.0.0-20220609144429-65e65417b02f // indirect
	google.golang.org/api v0.85.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20220725144611-272f38e5d71b // indirect
	google.golang.org/grpc v1.48.0 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/alexcesaro/statsd.v1 v1.0.0-20160306065229-c289775e46fd // indirect
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/ini.v1 v1.51.0 // indirect
	gopkg.in/jcmturner/aescts.v1 v1.0.1 // indirect
	gopkg.in/jcmturner/dnsutils.v1 v1.0.1 // indirect
	gopkg.in/jcmturner/gokrb5.v7 v7.5.0 // indirect
	gopkg.in/jcmturner/rpc.v1 v1.1.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.0 // indirect
)
