#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
CP="$PROJECT_DIR/target/classes:$PROJECT_DIR/target/test-classes:$PROJECT_DIR/benchmark-libs/*"
MAIN="io.github.beingmartinbmc.pravaah.BenchmarkRunner"
RESULTS_DIR="$PROJECT_DIR/benchmark-results"
mkdir -p "$RESULTS_DIR"

JDK8="/Users/sharma.ankit2/Downloads/openjdk1.8.0.491_8.93.0.18_aarch64/bin/java"
JDK11="/Users/sharma.ankit2/Library/Java/JavaVirtualMachines/jbr_dcevm-11.0.16/Contents/Home/bin/java"
JDK17="$(/usr/libexec/java_home -v 17)/bin/java"
JDK25="/Users/sharma.ankit2/Downloads/sfdc-jdk-zulu-25.0.1.0.101_17-macos_aarch64/bin/java"

declare -a JDKS=("JDK8:$JDK8" "JDK11:$JDK11" "JDK17:$JDK17" "JDK25:$JDK25")

for entry in "${JDKS[@]}"; do
    LABEL="${entry%%:*}"
    JAVA="${entry#*:}"
    echo ""
    echo "######################################################################"
    echo "# Running benchmark with $LABEL"
    echo "######################################################################"
    echo ""

    OUTFILE="$RESULTS_DIR/$LABEL.txt"
    "$JAVA" -Xmx6g -cp "$CP" "$MAIN" 2>&1 | tee "$OUTFILE"

    echo ""
    echo ">>> Saved to $OUTFILE"
    echo ""
done

echo ""
echo "All benchmarks complete. Results in $RESULTS_DIR/"
