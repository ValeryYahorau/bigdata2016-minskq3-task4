package com.epam.bigdata2016.minskq3.task4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class SecondarySortJobTest {

    MapDriver<Object, Text, CustomKey, Text> mapDriver;
    ReduceDriver<CustomKey, Text, NullWritable, Text> reduceDriver;
    MapReduceDriver<Object, Text, CustomKey, Text, NullWritable, Text> mapReduceDriver;

    private final String input1 = "8c66f1538798b7ab57e2da7be11c5696	20130606222224944	Z0KpO7S8PQpNDBa	Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)	116.1.44.*	276	279	3	tMxYQ19aM98	8318e6e7deadc6405ee01f01d31d985a	null	LV_1001_LDVi_LD_ADX_2	300	250	0	0	100	e1af08818a6cd6bbba118bb54a651961	254	3476	282825712806	1";
    private final String input2 = "2d34c0a50472ba3a4e3c83903437eae0	20130606222224951	Vh16L7SiOo1hJCC	Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	116.1.44.*	238	241	3	tMxYQ19aM98	202f6e8052731b38647f987dbc4a5db	null	LV_1001_LDVi_LD_ADX_1	300	250	0	0	100	00fccc64a1ee2809348509b7ac2a97a5	241	3427	282825712767	1";
    private final String input3 = "8c66f1538798b7ab57e2da7be11c5696	20130606222224943	Z0KpO7S8PQpNDBa	Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)	116.1.44.*	276	279	3	tMxYQ19aM98	8318e6e7deadc6405ee01f01d31d985a	null	LV_1001_LDVi_LD_ADX_2	300	250	0	0	100	e1af08818a6cd6bbba118bb54a651961	254	3476	282825712806	1";
    private final String input4 = "2d34c0a50472ba3a4e3c83903437eae0	20130606222224950	Vh16L7SiOo1hJCC	Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	116.1.44.*	238	241	3	tMxYQ19aM98	202f6e8052731b38647f987dbc4a5db	null	LV_1001_LDVi_LD_ADX_1	300	250	0	0	100	00fccc64a1ee2809348509b7ac2a97a5	241	3427	282825712767	1";

    private final String ip1 = "Z0KpO7S8PQpNDBa";
    private final long timestamp1 = 20130606222224944L;
    private final long timestamp3 = 20130606222224943L;
    private final String ip2 = "Vh16L7SiOo1hJCC";
    private final long timestamp2 = 20130606222224951L;
    private final long timestamp4 = 20130606222224950L;


    @Before
    public void setUp() {
        SecondarySortJob.LogsMapper mapper = new SecondarySortJob.LogsMapper();
        SecondarySortJob.LogsReducer reducer = new SecondarySortJob.LogsReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(input1));
        mapDriver.withInput(new LongWritable(), new Text(input2));
        mapDriver.withInput(new LongWritable(), new Text(input3));
        mapDriver.withInput(new LongWritable(), new Text(input4));
        mapDriver.withOutput(new CustomKey(ip1, timestamp1), new Text(input1));
        mapDriver.withOutput(new CustomKey(ip2, timestamp2), new Text(input2));
        mapDriver.withOutput(new CustomKey(ip1, timestamp3), new Text(input3));
        mapDriver.withOutput(new CustomKey(ip2, timestamp4), new Text(input4));

        mapDriver.runTest();
    }

//    @Test
//    public void testReducer() throws IOException {
//        List<CustomKey> values1 = new ArrayList<CustomKey>();
//        values1.add(new CustomKey(1, 254));
//        values1.add(new CustomKey(1, 254));
//        reduceDriver.withInput(new Text(ip1), values1);
//
//        List<CustomKey> values2 = new ArrayList<CustomKey>();
//        values2.add(new CustomKey(1, 254));
//        reduceDriver.withInput(new Text(ip2), values2);
//
//        reduceDriver.withOutput(new Text(ip1), new CustomKey(2, 508));
//        reduceDriver.withOutput(new Text(ip2), new CustomKey(1, 254));
//        reduceDriver.runTest();
//    }
//
//    @Test
//    public void testMapReduce() throws IOException {
//        mapReduceDriver.withInput(new LongWritable(), new Text(input1));
//        mapReduceDriver.withInput(new LongWritable(), new Text(input2));
//        mapReduceDriver.withInput(new LongWritable(), new Text(input3));
//
//        mapReduceDriver.withOutput(new Text(ip1), new CustomKey(2, 508));
//        mapReduceDriver.withOutput(new Text(ip2), new CustomKey(1, 254));
//
//        mapReduceDriver.runTest();
//    }
}