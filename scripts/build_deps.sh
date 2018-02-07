# build librdkafka and export the according pkg config so it will be found

if [ -z $TMP_DIR ]
then
  TMP_DIR=$(mktemp -d)
else
	if ! [ -d $TMP_DIR ]
	then
		mkdir -p $TMP_DIR
	fi
fi

SOURCE_DIR=$(dirname ${BASH_SOURCE[0]})/..
LIB_RDKAFKA_DIR=$SOURCE_DIR/vendor/github.com/edenhill/librdkafka
cd $LIB_RDKAFKA_DIR
./configure --prefix=$TMP_DIR
make
make install
make clean

export PKG_CONFIG_PATH=$TMP_DIR/lib/pkgconfig

if [ -z $LD_LIBRARY_PATH ]
then
	export LD_LIBRARY_PATH=$TMP_DIR/lib
else
	export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$TMP_DIR/lib"
fi

cd $OLDPWD

# cleanup
rm \
	vendor/github.com/edenhill/librdkafka/Makefile.config \
	vendor/github.com/edenhill/librdkafka/config.cache \
	vendor/github.com/edenhill/librdkafka/config.h \
	vendor/github.com/edenhill/librdkafka/config.log \
	vendor/github.com/edenhill/librdkafka/config.log.old

# this file gets modified when configuring the package, which then causes
# tests to fail because it differs from what's committed in git. so we check
# it out to not make the tests fail.
git checkout vendor/github.com/edenhill/librdkafka/CONFIGURATION.md
