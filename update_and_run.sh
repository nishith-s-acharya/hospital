#!/bin/bash

echo "==========================================="
echo " Updating CSV from Google Sheets"
echo "==========================================="

# Your Google Sheet CSV link (replace with your correct link)
CSV_URL="https://docs.google.com/spreadsheets/d/e/2PACX-1vTalZNrqlO5ats_tAlnNOWItVFl7sNrtoFVS2I-JKgwxRhU03X2yHrfUoXcEW1IShSGKRNH_rREVu04/pub?gid=1183394350&single=true&output=csv"

mkdir -p input

echo ">>> Downloading latest patient_visits.csv ..."
wget -q -O input/patient_visits.csv "$CSV_URL"

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

export HADOOP_CONF_DIR=/home/hadoop/tmp_hadoop_conf
mkdir -p /home/hadoop/tmp_hadoop_conf

# 1️⃣ Length of Stay
echo ">>> Running Length of Stay KPI..."
/home/hadoop/Hadoop/bin/hadoop \
  jar healthcare_kpi.jar \
  org.healthcare.kpi.LengthOfStayMR \
  input/patient_visits.csv \
  output/los

# 2️⃣ Frequent Diagnoses
echo ">>> Running Frequent Diagnoses KPI..."
/home/hadoop/Hadoop/bin/hadoop \
  jar healthcare_kpi.jar \
  org.healthcare.kpi.FrequentDiagnosesMR \
  input/patient_visits.csv \
  output/diag_counts

# 3️⃣ Readmission Rate
echo ">>> Running Readmission Rate KPI..."
/home/hadoop/Hadoop/bin/hadoop \
  jar healthcare_kpi.jar \
  org.healthcare.kpi.ReadmissionRateMR \
  input/patient_visits.csv \
  output/readmission

echo ""
echo "==========================================="
echo " Latest KPI Results"
echo "==========================================="

echo "--- Length of Stay ---"
cat output/los/part-r-00000

echo ""
echo "--- Frequent Diagnoses ---"
cat output/diag_counts/part-r-00000

echo ""
echo "--- Readmission Rate ---"
cat output/readmission/part-r-00000

echo "==========================================="
