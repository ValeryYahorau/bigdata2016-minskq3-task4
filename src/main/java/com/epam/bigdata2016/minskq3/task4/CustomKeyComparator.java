package com.epam.bigdata2016.minskq3.task4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomKeyComparator extends WritableComparator {

    public CustomKeyComparator() {
        super(CustomKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CustomKey cik1 = (CustomKey) a;
        CustomKey cik2 = (CustomKey) b;
        int iPinyouIDCmp = cik1.getiPinyouID().compareToIgnoreCase(cik2.getiPinyouID());
        if (iPinyouIDCmp != 0 ) {
            return iPinyouIDCmp;
        }
        return Long.compare(cik1.getTimestam(),cik2.getTimestam());
    }
}
