package org.healthcare.kpi;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

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

public class LengthOfStayMR {

    public static class LOSMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text deptKey = new Text();
        private final IntWritable losValue = new IntWritable();
        private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd/MM/yyyy");

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] fields = line.split(",");

            // Skip header or malformed lines
            if (fields.length < 8) {
                return;
            }
            if (fields[0].toLowerCase().contains("timestamp")) {
                return; // header row
            }

            // Google Sheet layout:
            // 0 - Timestamp
            // 1 - Visit ID
            // 2 - Patient ID
            // 3 - Admission Date
            // 4 - Discharge Date
            // 5 - Department
            // 6 - Visit Type
            // 7 - LOS (optional)
            String admissionDate = fields[3].trim();
            String dischargeDate = fields[4].trim();
            String department = fields[5].trim();

            if (admissionDate.isEmpty() || dischargeDate.isEmpty() || department.isEmpty()) {
                return; // incomplete record
            }

            long los = 0;
            try {
                // Prefer LOS from column 7 if present
                if (!fields[7].trim().isEmpty()) {
                    los = Long.parseLong(fields[7].trim());
                } else {
                    LocalDate start = LocalDate.parse(admissionDate, FORMATTER);
                    LocalDate end = LocalDate.parse(dischargeDate, FORMATTER);
                    los = ChronoUnit.DAYS.between(start, end);
                }
            } catch (Exception e) {
                // if any parsing fails, skip this row
                return;
            }

            // Ignore non-positive LOS and blank departments
            if (los <= 0 || department.isEmpty()) {
                return;
            }

            deptKey.set(department);
            losValue.set((int) los);
            context.write(deptKey, losValue);
        }
    }

    public static class LOSReducer extends Reducer<Text, IntWritable, Text, Text> {

        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int total = 0;
            int count = 0;

            for (IntWritable v : values) {
                total += v.get();
                count++;
            }

            if (count == 0) {
                return;
            }

            double avg = (double) total / count;
            String summary = String.format(
                    "Total_LOS_Days=%d, Visit_Count=%d, Avg_LOS_Days=%.2f",
                    total, count, avg);

            outValue.set(summary);
            context.write(key, outValue);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: LengthOfStayMR <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Length Of Stay KPI");

        job.setJarByClass(LengthOfStayMR.class);
        job.setMapperClass(LOSMapper.class);
        job.setReducerClass(LOSReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
