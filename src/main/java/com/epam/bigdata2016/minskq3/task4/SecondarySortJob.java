package com.epam.bigdata2016.minskq3.task4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SecondarySortJob {

    public static class LogsMapper extends Mapper<Object, Text, CustomKey, Text> {

        private Text logLine = new Text();
        private CustomKey cikw = new CustomKey();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] params = line.split("\\s+");

            logLine.set(line);
            cikw.setiPinyouID(params[2]);
            cikw.setTimestam(Long.parseLong(params[1]));

            context.write(cikw, logLine);
        }
    }

    public static class LogsReducer extends Reducer<CustomKey, Text, NullWritable, Text> {

        NullWritable nullKey = NullWritable.get();
        private long maxSiteImpressionSum = 0;
        private List<String> iPinyouIdList = new ArrayList<>();

        public void reduce(CustomKey key, Iterable<Text> values, Reducer<CustomKey, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {

            long siteImpressionSum = 0;
            for (Text val : values) {
                context.write(nullKey, val);

                String logLine = val.toString();
                int streamId = Integer.parseInt(logLine.substring(logLine.length() - 1));
                if (streamId == 1) {
                    ++siteImpressionSum;
                }
            }

            if (maxSiteImpressionSum == siteImpressionSum && !"null".equals(key.getiPinyouID())) {
                iPinyouIdList.add(key.getiPinyouID());
            } else if (maxSiteImpressionSum < siteImpressionSum && !"null".equals(key.getiPinyouID())) {
                iPinyouIdList.clear();
                iPinyouIdList.add(key.getiPinyouID());
                maxSiteImpressionSum = siteImpressionSum;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (iPinyouIdList.size() > 0) {
                for (String currentIPinyouId : iPinyouIdList) {
                    context.getCounter("DynamicCounter", currentIPinyouId).setValue(maxSiteImpressionSum);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //otherArgs = new String[]{"/Users/valeryyegorov/Downloads/in2.txt", "/Users/valeryyegorov/Downloads/out2.txt"};

        if (otherArgs.length < 2) {
            System.err.println("Usage: VisitsSpendsCount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Visits Spends count");
        job.setJarByClass(SecondarySortJob.class);
        job.setMapperClass(LogsMapper.class);
        // job.setCombinerClass(LogsReducer.class);
        job.setReducerClass(LogsReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(CustomKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(CustomKeyPartitioner.class);
        job.setSortComparatorClass(CustomKeyComparator.class);
        job.setGroupingComparatorClass(CustomKeyGroupingComparator.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        boolean result = job.waitForCompletion(true);

        Counters counters = job.getCounters();

        //System.out.println("iPinyou ID with the biggest ammount of site-impression :");
        for (Counter counter : counters.getGroup("DynamicCounter")) {
            System.out.println("iPinyou ID: " + counter.getName() + ", the biggest amount of site impression: " + counter.getValue());
        }

        System.exit(result ? 0 : 1);
    }
}