#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
JAR="$PROJECT_DIR/target/pravaah-java-1.0.0-SNAPSHOT.jar"
CP="$JAR:$PROJECT_DIR/target/test-classes:$PROJECT_DIR/benchmark-libs/*"
MAIN="io.github.beingmartinbmc.pravaah.BenchmarkRunner"
RESULTS_DIR="$PROJECT_DIR/benchmark-results"
mkdir -p "$RESULTS_DIR"

JDK8="/Users/sharma.ankit2/Downloads/openjdk1.8.0.491_8.93.0.18_aarch64/bin/java"
JDK11="/Users/sharma.ankit2/Library/Java/JavaVirtualMachines/jbr_dcevm-11.0.16/Contents/Home/bin/java"
JDK17="$(/usr/libexec/java_home -v 17)/bin/java"
JDK25="/Users/sharma.ankit2/Downloads/sfdc-jdk-zulu-25.0.1.0.101_17-macos_aarch64/bin/java"

if ! ls "$PROJECT_DIR"/benchmark-libs/*.jar >/dev/null 2>&1; then
    echo "Missing benchmark-libs/*.jar. Download competitor JARs first (see README)."
    exit 1
fi

echo "Packaging Pravaah MR-JAR and compiling benchmark runner..."
mkdir -p "$PROJECT_DIR/target/test-classes"
JAVAC8="${JDK8%/bin/java}/bin/javac"
"$JDK8" -version >/dev/null 2>&1
(cd "$PROJECT_DIR" && mvn -q -DskipTests package)
"$JAVAC8" -source 1.8 -target 1.8 \
    -cp "$JAR:$PROJECT_DIR/benchmark-libs/*" \
    -d "$PROJECT_DIR/target/test-classes" \
    "$PROJECT_DIR/src/test/java/io/github/beingmartinbmc/pravaah/BenchmarkRunner.java"

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
