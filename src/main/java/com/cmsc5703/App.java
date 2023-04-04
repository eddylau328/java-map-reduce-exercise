package com.cmsc5703;

import java.sql.Timestamp;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class App 
{

    public static void main( String[] args )
    {
        // Create a configuration object
        JobConf conf = new JobConf(App.class);
        conf.setJobName("HTML-Link-Counter");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(HTMLLinkMapper.class);
        conf.setReducerClass(HTMLLinkReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        // args[0] = name of the input directory
        // args[1] = output file (result of the sorted list)
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1])); 

        // Testing
        // Date date = new Date();
        // Timestamp timestamp = new Timestamp(date.getTime());
        // String outputPathString = "./output/" + timestamp.toString();
        // FileInputFormat.setInputPaths(conf, new Path("./input"));
        // FileOutputFormat.setOutputPath(conf, new Path(outputPathString));

        try {
            // Run job
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
