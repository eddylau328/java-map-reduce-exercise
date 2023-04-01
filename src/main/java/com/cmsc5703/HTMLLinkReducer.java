package com.cmsc5703;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class HTMLLinkReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(
        Text key,
        Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output,
        Reporter reporter
    ) throws IOException {
        int count = 0;
        while (values.hasNext()) {
            IntWritable value = values.next();
            count += value.get();
        }
        if (count <= 5) return;
        output.collect(key, new IntWritable(count));
    }
}
