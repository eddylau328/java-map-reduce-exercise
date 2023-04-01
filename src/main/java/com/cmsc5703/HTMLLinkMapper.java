package com.cmsc5703;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;

import net.htmlparser.jericho.Source;
import net.htmlparser.jericho.StartTag;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class HTMLLinkMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable> {
    public void map(
        LongWritable key,
        Text value,
        OutputCollector <Text, IntWritable> output,
        Reporter reporter
    ) throws IOException {
        Source source = new Source(value.toString());
        List<StartTag> tags = source.getAllStartTags("a");
        HashMap<String, Integer> linkCount = new HashMap<String, Integer>();
        for (StartTag tag : tags) {
            String link = tag.getAttributeValue("href");
            if (link == null) continue;
            Integer prevCount = linkCount.get(link);
            if (prevCount == null) linkCount.put(link, 1);
            else linkCount.put(link, prevCount + 1);
        }
        for (Map.Entry<String, Integer> entry : linkCount.entrySet()) {
            output.collect(new Text(entry.getKey()), new IntWritable(entry.getValue()));
        }
    }
}

