package com.cmsc5703;

import java.nio.ByteBuffer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingIntComparator extends WritableComparator {

    public DescendingIntComparator() {
        super(IntWritable.class);
    }

    @Override
    public int compare(
        byte[] b1, int s1, int l1,
        byte[] b2, int s2, int l2
    ) {
        ByteBuffer buf1 = ByteBuffer.wrap(b1, s1, l1);
        ByteBuffer buf2 = ByteBuffer.wrap(b2, s2, l2);
        byte[] arr1 = new byte[l1 < 4 ? 4 : l1];
        byte[] arr2 = new byte[l2 < 4 ? 4 : l2];
        buf1.get(arr1, arr1.length - l1, l1);
        buf2.get(arr2, arr2.length - l2, l2);
        Integer v1 = ByteBuffer.wrap(arr1).getInt();
        Integer v2 = ByteBuffer.wrap(arr2).getInt();
        return v1.compareTo(v2) * (-1);
    }
}
