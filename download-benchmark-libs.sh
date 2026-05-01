#!/bin/bash
set -e

DIR="$(cd "$(dirname "$0")" && pwd)/benchmark-libs"
REPO="https://repo1.maven.org/maven2"
mkdir -p "$DIR"

download() {
    local path="$1"
    local jar="${path##*/}"
    if [ ! -s "$DIR/$jar" ]; then
        echo "Downloading $jar"
        curl -sL "$REPO/$path" -o "$DIR/$jar"
    fi
}

# CSV competitors
download "com/univocity/univocity-parsers/2.9.1/univocity-parsers-2.9.1.jar"
download "org/apache/commons/commons-csv/1.12.0/commons-csv-1.12.0.jar"
download "com/opencsv/opencsv/5.9/opencsv-5.9.jar"
download "com/fasterxml/jackson/dataformat/jackson-dataformat-csv/2.18.4/jackson-dataformat-csv-2.18.4.jar"
download "com/fasterxml/jackson/core/jackson-core/2.18.4.1/jackson-core-2.18.4.1.jar"
download "com/fasterxml/jackson/core/jackson-databind/2.18.4/jackson-databind-2.18.4.jar"
download "com/fasterxml/jackson/core/jackson-annotations/2.18.4/jackson-annotations-2.18.4.jar"
download "org/apache/commons/commons-lang3/3.17.0/commons-lang3-3.17.0.jar"
download "org/apache/commons/commons-text/1.13.0/commons-text-1.13.0.jar"
download "org/apache/commons/commons-collections4/4.5.0-M3/commons-collections4-4.5.0-M3.jar"
download "commons-io/commons-io/2.18.0/commons-io-2.18.0.jar"
download "commons-codec/commons-codec/1.17.2/commons-codec-1.17.2.jar"
download "commons-beanutils/commons-beanutils/1.9.4/commons-beanutils-1.9.4.jar"
download "commons-logging/commons-logging/1.3.5/commons-logging-1.3.5.jar"

# Spreadsheet competitors: Apache POI XSSF and EasyExcel
download "org/apache/poi/poi/5.2.5/poi-5.2.5.jar"
download "org/apache/poi/poi-ooxml/5.2.5/poi-ooxml-5.2.5.jar"
download "org/apache/poi/poi-ooxml-lite/5.2.5/poi-ooxml-lite-5.2.5.jar"
download "org/apache/xmlbeans/xmlbeans/5.2.0/xmlbeans-5.2.0.jar"
download "org/apache/commons/commons-compress/1.25.0/commons-compress-1.25.0.jar"
download "com/github/virtuald/curvesapi/1.08/curvesapi-1.08.jar"
download "com/zaxxer/SparseBitSet/1.3/SparseBitSet-1.3.jar"
download "org/apache/logging/log4j/log4j-api/2.21.1/log4j-api-2.21.1.jar"
download "org/apache/logging/log4j/log4j-core/2.21.1/log4j-core-2.21.1.jar"
download "com/alibaba/easyexcel/4.0.3/easyexcel-4.0.3.jar"
download "com/alibaba/easyexcel-core/4.0.3/easyexcel-core-4.0.3.jar"
download "com/alibaba/easyexcel-support/3.3.4/easyexcel-support-3.3.4.jar"
download "org/ehcache/ehcache/3.9.11/ehcache-3.9.11.jar"
download "org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar"
download "org/slf4j/slf4j-nop/1.7.36/slf4j-nop-1.7.36.jar"

echo "Benchmark libraries are ready in $DIR"
