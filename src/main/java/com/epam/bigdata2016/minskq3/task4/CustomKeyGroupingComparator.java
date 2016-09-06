package com.epam.bigdata2016.minskq3.task4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomKeyGroupingComparator extends WritableComparator {

    public CustomKeyGroupingComparator() {
        super(CustomKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CustomKey cik1 = (CustomKey) a;
        CustomKey cik2 = (CustomKey) b;
        return cik1.getiPinyouID().compareToIgnoreCase(cik2.getiPinyouID());
    }
}
