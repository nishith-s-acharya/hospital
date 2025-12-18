package org.healthcare.kpi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * FrequentDiagnosesMR
 *
 * Counts how many times each diagnosis occurs.
 *
 * Works with Google Sheet CSV:
 *  - It reads the header row.
 *  - Finds the column whose header contains "diagnos" (Diagnosis / Diagnosis Name).
 *  - Uses that column as the diagnosis name for counting.
 *
 * Also still supports old 3-column diagnoses.csv files:
 *   Visit_ID,Diagnosis_ID,Diagnosis_Name
 */
public class FrequentDiagnosesMR {

    public static class DiagnosisMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text outKey = new Text();
        private static final IntWritable ONE = new IntWritable(1);

        // index of Diagnosis column in CSV (from header)
        private int diagnosisIndex = -1;
        private boolean headerSeen = false;

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] fields = line.split(",", -1);

            // First non-empty line is treated as header
            if (!headerSeen) {
                headerSeen = true;

                // Try to detect "Diagnosis" column by name
                for (int i = 0; i < fields.length; i++) {
                    String header = fields[i].trim().toLowerCase();
                    if (header.contains("diagnos")) { // matches "Diagnosis", "Diagnosis Name", etc.
                        diagnosisIndex = i;
                        break;
                    }
                }

                // This line is header, so don't emit anything
                return;
            }

            String diagnosisName = null;

            // If we successfully found a "Diagnosis" column in header
            if (diagnosisIndex >= 0 && diagnosisIndex < fields.length) {
                diagnosisName = fields[diagnosisIndex].trim();
            } else if (fields.length == 3) {
                // Fallback: classic diagnoses.csv format: Visit_ID,Diagnosis_ID,Diagnosis_Name
                diagnosisName = fields[2].trim();
            } else if (fields.length >= 1) {
                // Last fallback: use last column (not ideal, but better than nothing)
                diagnosisName = fields[fields.length - 1].trim();
            }

            if (diagnosisName == null) {
                return;
            }

            diagnosisName = diagnosisName.trim();
            if (diagnosisName.isEmpty()) {
                return;
            }

            // Extra safety: ignore cells that literally look like headers
            String dl = diagnosisName.toLowerCase();
            if (dl.contains("diagnos")) {
                return;
            }

            outKey.set(diagnosisName);
            context.write(outKey, ONE);
        }
    }

    public static class DiagnosisReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable outValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            outValue.set(sum);
            context.write(key, outValue);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: FrequentDiagnosesMR <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Frequent Diagnoses KPI");
        job.setJarByClass(FrequentDiagnosesMR.class);

        job.setMapperClass(DiagnosisMapper.class);
        job.setReducerClass(DiagnosisReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
