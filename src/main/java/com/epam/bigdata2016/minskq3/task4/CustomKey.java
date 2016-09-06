package com.epam.bigdata2016.minskq3.task4;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomKey implements WritableComparable<CustomKey> {

    private String iPinyouID;
    private long timestam;

    public CustomKey() {
    }

    public CustomKey(String iPinyouID, long timestam) {
        this.iPinyouID = iPinyouID;
        this.timestam = timestam;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(iPinyouID);
        out.writeLong(timestam);
    }

    public void readFields(DataInput in) throws IOException {
        iPinyouID = in.readUTF();
        timestam = in.readLong();
    }

    @Override
    public int compareTo(CustomKey w) {
        int iPinyouIDCmp = iPinyouID.compareToIgnoreCase(w.iPinyouID);
        if (iPinyouIDCmp != 0 ) {
            return iPinyouIDCmp;
        }
        return Long.compare(timestam,w.timestam);
    }

    public String getiPinyouID() {
        return iPinyouID;
    }

    public void setiPinyouID(String iPinyouID) {
        this.iPinyouID = iPinyouID;
    }

    public long getTimestam() {
        return timestam;
    }

    public void setTimestam(long timestam) {
        this.timestam = timestam;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CustomKey that = (CustomKey) o;

        if (getTimestam() != that.getTimestam()) return false;
        return getiPinyouID().equals(that.getiPinyouID());

    }

    @Override
    public int hashCode() {
        int result = getiPinyouID().hashCode();
        result = 31 * result + (int) (getTimestam() ^ (getTimestam() >>> 32));
        return result;
    }
}