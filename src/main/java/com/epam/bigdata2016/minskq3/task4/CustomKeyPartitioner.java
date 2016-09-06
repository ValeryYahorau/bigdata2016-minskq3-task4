package com.epam.bigdata2016.minskq3.task4;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomKeyPartitioner extends Partitioner<CustomKey, NullWritable> {

    @Override
    public int getPartition(CustomKey customKey, NullWritable value, int numPartitions) {
        return customKey.getiPinyouID().hashCode() % numPartitions;
    }
}
