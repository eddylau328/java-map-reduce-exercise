package com.cmsc5703;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class HTMLLinkReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    private TreeMap<Integer, ArrayList<String>> treeMap = new TreeMap<>();
    private OutputCollector<Text, IntWritable> collector = null;

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
        ArrayList<String> links = treeMap.get(count);
        if (links == null) links = new ArrayList<String>();
        links.add(key.toString());
        treeMap.put(count, links);
        collector = output;
    }

    @Override
    public void close() throws IOException {
        Set<Integer> keySet = treeMap.descendingKeySet();
        for (Integer key : keySet) {
            treeMap.get(key).sort(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    if(o1.length() < o2.length()){
                        return 1;
                    }else if (o1.length() > o2.length()) {
                        return -1;
                    } else {
                        return 0;
                    }
                }
            });
            for (String link : treeMap.get(key)) {
                collector.collect(new Text(link), new IntWritable(key));
            }
        }
    }
}
