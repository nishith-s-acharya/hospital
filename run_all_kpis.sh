#!/bin/bash

echo ">>> Building Hadoop classpath..."
export HADOOP_HOME=/home/hadoop/Hadoop
export CP="$(find "$HADOOP_HOME"/share/hadoop -name '*.jar' -printf '%p:' 2>/dev/null)healthcare_kpi.jar"

echo ">>> Cleaning old outputs..."
rm -rf output/los output/diag_counts output/readmission

echo "=================================================="
echo " Running ALL KPIs (Local Mode – No Hadoop services)"
echo "=================================================="

# ------------------------------
# 1) Length of Stay
# ------------------------------
echo ">>> Running Length of Stay KPI..."
java -cp "$CP" -Dfs.defaultFS=file:/// -Dmapreduce.framework.name=local \
  org.healthcare.kpi.LengthOfStayMR \
  input/patient_visits.csv output/los

# ------------------------------
# 2) Frequent Diagnoses
# ------------------------------
echo ">>> Running Frequent Diagnoses KPI..."
java -cp "$CP" -Dfs.defaultFS=file:/// -Dmapreduce.framework.name=local \
  org.healthcare.kpi.FrequentDiagnosesMR \
  input/diagnoses.csv output/diag_counts

# ------------------------------
# 3) Readmission Rate
# ------------------------------
echo ">>> Running Readmission Rate KPI..."
java -cp "$CP" -Dfs.defaultFS=file:/// -Dmapreduce.framework.name=local \
  org.healthcare.kpi.ReadmissionRateMR \
  input/patient_visits.csv output/readmission

echo ""
echo "=================================================="
echo "        FINAL KPI OUTPUTS (ALL AT ONCE)"
echo "=================================================="

echo ""
echo "--- Length of Stay (LOS) ---"
cat output/los/part-r-00000 2>/dev/null || echo "LOS output missing"

echo ""
echo "--- Frequent Diagnoses ---"
cat output/diag_counts/part-r-00000 2>/dev/null || echo "Diagnosis output missing"

echo ""
echo "--- Readmission Rate ---"
cat output/readmission/part-r-00000 2>/dev/null || echo "Readmission output missing"

echo ""
echo "=================================================="
echo "                 END OF REPORT"
echo "=================================================="
