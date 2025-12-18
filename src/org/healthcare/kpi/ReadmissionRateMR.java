package org.healthcare.kpi;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ReadmissionRateMR
 *
 * Uses patient_visits.csv with columns:
 * Timestamp,Visit ID,Patient ID,Admission Date,Discharge Date,Department,Visit Type,Length_of_Stay_Days
 *
 * Logic:
 *  - Mapper keys by Patient ID.
 *  - Reducer sorts visits by Admission Date and checks if any later visit
 *    occurs within 30 days of previous Discharge.
 *  - Outputs READMITTED / NOT_READMITTED per patient + one OVERALL_SUMMARY row.
 */
public class ReadmissionRateMR {

    // ---------- MAPPER ----------
    public static class VisitMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] fields = line.split(",");

            // Expect at least 8 columns for valid data
            if (fields.length < 8) {
                return;
            }

            // Skip header row: starts with "Timestamp"
            if (fields[0].toLowerCase().contains("timestamp")) {
                return;
            }

            // 0 - Timestamp
            // 1 - Visit ID
            // 2 - Patient ID
            // 3 - Admission Date (dd/MM/yyyy)
            // 4 - Discharge Date (dd/MM/yyyy)
            // 5 - Department
            // 6 - Visit Type
            // 7 - LOS days (optional, not needed here)
            String visitId = fields[1].trim();
            String patientId = fields[2].trim();
            String admissionDate = fields[3].trim();
            String dischargeDate = fields[4].trim();

            if (patientId.isEmpty() || admissionDate.isEmpty() || dischargeDate.isEmpty()) {
                return;
            }

            outKey.set(patientId);
            // pack visit data as "visitId|admissionDate|dischargeDate"
            outValue.set(visitId + "|" + admissionDate + "|" + dischargeDate);
            context.write(outKey, outValue);
        }
    }

    // ---------- REDUCER ----------
    public static class ReadmissionReducer extends Reducer<Text, Text, Text, Text> {

        private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd/MM/yyyy");

        private int totalPatients = 0;
        private int readmittedPatients = 0;

        private final Text outValue = new Text();

        // Helper class for visits
        private static class Visit {
            LocalDate admission;
            LocalDate discharge;
            String visitId;

            Visit(LocalDate admission, LocalDate discharge, String visitId) {
                this.admission = admission;
                this.discharge = discharge;
                this.visitId = visitId;
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<Visit> visits = new ArrayList<>();

            // Parse all visit strings for this patient
            for (Text t : values) {
                String[] parts = t.toString().split("\\|");
                if (parts.length != 3) {
                    continue;
                }

                String visitId = parts[0].trim();
                String admissionStr = parts[1].trim();
                String dischargeStr = parts[2].trim();

                try {
                    LocalDate admission = LocalDate.parse(admissionStr, FORMATTER);
                    LocalDate discharge = LocalDate.parse(dischargeStr, FORMATTER);
                    visits.add(new Visit(admission, discharge, visitId));
                } catch (Exception e) {
                    // Skip invalid date rows
                }
            }

            if (visits.isEmpty()) {
                outValue.set("NO_VALID_VISITS");
                context.write(key, outValue);
                return;
            }

            // Sort visits by admission date
            Collections.sort(visits, new Comparator<Visit>() {
                @Override
                public int compare(Visit v1, Visit v2) {
                    return v1.admission.compareTo(v2.admission);
                }
            });

            boolean readmitted = false;

            // Check if any later visit happens within 30 days of the previous discharge
            for (int i = 1; i < visits.size(); i++) {
                Visit prev = visits.get(i - 1);
                Visit curr = visits.get(i);

                long gap = ChronoUnit.DAYS.between(prev.discharge, curr.admission);
                if (gap >= 0 && gap <= 30) {
                    readmitted = true;
                    break;
                }
            }

            totalPatients++;
            if (readmitted) {
                readmittedPatients++;
            }

            outValue.set(readmitted ? "READMITTED" : "NOT_READMITTED");
            context.write(key, outValue);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Text summaryKey = new Text("OVERALL_SUMMARY");

            if (totalPatients > 0) {
                double rate = (readmittedPatients * 100.0) / totalPatients;
                String summary = String.format(
                        "Readmitted_Patients=%d, Total_Patients=%d, Readmission_Rate(%%)=%.2f",
                        readmittedPatients, totalPatients, rate);
                context.write(summaryKey, new Text(summary));
            } else {
                context.write(summaryKey, new Text(
                        "Readmitted_Patients=0, Total_Patients=0, Readmission_Rate(%)=0.00"));
            }
        }
    }

    // ---------- DRIVER ----------
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: ReadmissionRateMR <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Readmission Rate KPI");

        job.setJarByClass(ReadmissionRateMR.class);
        job.setMapperClass(VisitMapper.class);
        job.setReducerClass(ReadmissionReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
