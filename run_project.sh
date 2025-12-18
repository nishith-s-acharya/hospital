#!/bin/bash

# Detect OS and set JAVA_HOME
OS="$(uname -s)"
if [ "$OS" = "Darwin" ]; then
    # macOS
    export JAVA_HOME=$(/usr/libexec/java_home -v 17)
elif [ "$OS" = "Linux" ]; then
    # Linux (Ubuntu/Debian)
    if [ -d "/usr/lib/jvm/java-17-openjdk-amd64" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
    elif [ -d "/usr/lib/jvm/java-17-openjdk" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-17-openjdk"
    fi
fi

echo "Using JAVA_HOME: $JAVA_HOME"

# Check if JAVA_HOME is valid
if [ -z "$JAVA_HOME" ]; then
    echo "Java 17 not found! Please ensure Java 17 is installed."
    echo "On Ubuntu: sudo apt install openjdk-17-jdk"
    exit 1
fi

# Define constants
CSV_URL="https://docs.google.com/spreadsheets/d/e/2PACX-1vTalZNrqlO5ats_tAlnNOWItVFl7sNrtoFVS2I-JKgwxRhU03X2yHrfUoXcEW1IShSGKRNH_rREVu04/pub?gid=1183394350&single=true&output=csv"
JAR_NAME="healthcare_kpi.jar"

echo "==========================================="
echo " Updating CSV from Google Sheets"
echo "==========================================="

mkdir -p input

echo ">>> Downloading latest patient_visits.csv ..."
if command -v curl &> /dev/null; then
    curl -L -o input/patient_visits.csv "$CSV_URL"
elif command -v wget &> /dev/null; then
    wget -q -O input/patient_visits.csv "$CSV_URL"
else
    echo "Error: Neither curl nor wget found. Please install one of them."
    exit 1
fi

if [ $? -ne 0 ]; then
    echo "FAILED TO DOWNLOAD CSV. Check the URL."
    exit 1
fi

echo "CSV updated successfully."
echo ""

echo "==========================================="
echo " Running Hadoop KPIs (Local Mode)"
echo "==========================================="

# Cleanup old outputs
rm -rf output/los output/diag_counts output/readmission

echo "==========================================="
echo " Compiling Java Code"
echo "==========================================="

# Find Hadoop executable
if [ -n "$HADOOP_HOME" ] && [ -x "$HADOOP_HOME/bin/hadoop" ]; then
    HADOOP_CMD="$HADOOP_HOME/bin/hadoop"
elif command -v hadoop &> /dev/null; then
    HADOOP_CMD="hadoop"
else
    echo "Error: Hadoop not found!"
    echo "Please set HADOOP_HOME or ensure 'hadoop' is in your PATH."
    exit 1
fi

echo "Using Hadoop command: $HADOOP_CMD"

# Get Hadoop Classpath
export HADOOP_CLASSPATH=$($HADOOP_CMD classpath)

mkdir -p classes
echo ">>> Compiling sources..."
javac -d classes -cp "$HADOOP_CLASSPATH" src/org/healthcare/kpi/*.java

if [ $? -ne 0 ]; then
    echo "COMPILATION FAILED!"
    exit 1
fi

echo ">>> Creating JAR file..."
jar -cvf $JAR_NAME -C classes .

echo "Build successful."
echo ""

# 1 Length of Stay
echo ">>> Running Length of Stay KPI..."
"$HADOOP_CMD" jar $JAR_NAME \
  org.healthcare.kpi.LengthOfStayMR \
  input/patient_visits.csv \
  output/los

# 2 Frequent Diagnoses
echo ">>> Running Frequent Diagnoses KPI..."
"$HADOOP_CMD" jar $JAR_NAME \
  org.healthcare.kpi.FrequentDiagnosesMR \
  input/patient_visits.csv \
  output/diag_counts

# 3 Readmission Rate
echo ">>> Running Readmission Rate KPI..."
"$HADOOP_CMD" jar $JAR_NAME \
  org.healthcare.kpi.ReadmissionRateMR \
  input/patient_visits.csv \
  output/readmission

echo ""
echo "==========================================="
echo " Latest KPI Results"
echo "==========================================="

echo "--- Length of Stay ---"
cat output/los/part-r-00000 2>/dev/null

echo ""
echo "--- Frequent Diagnoses ---"
cat output/diag_counts/part-r-00000 2>/dev/null

echo ""
echo "--- Readmission Rate ---"
cat output/readmission/part-r-00000 2>/dev/null

echo "==========================================="
