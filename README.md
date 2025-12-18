# Healthcare Hadoop KPI Project

This project calculates Key Performance Indicators (KPIs) from healthcare patient visit data using Hadoop MapReduce.

## Prerequisites

- **Java**: Java 17 is required. The run script is configured to automatically find and use Java 17.
- **Hadoop**: Hadoop must be installed. The script uses the `HADOOP_HOME` environment variable or defaults to common installation paths.

## How to Run

I have created a helper script `run_project.sh` to automate the entire process:

1.  **Download Data**: It fetches the latest `patient_visits.csv` from the configured Google Sheet.
2.  **Run KPIs**: It executes three MapReduce jobs:
    -   **Length of Stay**: Calculates average length of stay per department.
    -   **Frequent Diagnoses**: Counts the occurrences of each diagnosis.
    -   **Readmission Rate**: Calculates the percentage of readmitted patients.
3.  **Show Results**: It prints the output of each KPI analysis to the console.

### Command

Run the following command in your terminal:

```bash
./run_project.sh
```

## Project Structure

-   `src/`: Java source code for the MapReduce jobs.
-   `input/`: Directory where the input CSV file is stored.
-   `output/`: Directory where the MapReduce job outputs are generated.
-   `healthcare_kpi.jar`: The compiled Hadoop JAR file.
-   `run_project.sh`: The main script to run the project.
