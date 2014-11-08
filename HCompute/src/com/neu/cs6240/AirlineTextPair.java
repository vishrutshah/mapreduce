package com.neu.cs6240;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.examples.SecondarySort.IntPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class AirlineTextPair implements WritableComparable {
    Text airLineName;    
    Text month;

    public AirlineTextPair() {
    	this.airLineName = new Text();
    	this.month = new Text();
    }

    public AirlineTextPair(Text airLineName, Text month) {
    	this.airLineName = airLineName;
    	this.month = month;
    }
   
    public void setAirLineName(String airLineName) {
    	this.airLineName.set(airLineName.getBytes());
    }   
   
    public void setMonth(String month) {
            this.month.set(month.getBytes());
    }

    public Text getAirLineName() {
            return this.airLineName;
    }

    public Text getMonth() {
            return this.month;
    }

    public void write(DataOutput out) throws IOException {
    	this.airLineName.write(out);
    	this.month.write(out);
    }

    public void readFields(DataInput in) throws IOException {
            if (this.airLineName == null)
            	this.airLineName = new Text();

            if (this.month == null)
            	this.month = new Text();

            this.airLineName.readFields(in);
            this.month.readFields(in);
    }

    public int compareTo(Object object) {
            AirlineTextPair ip2 = (AirlineTextPair) object;
            int cmp = getAirLineName().compareTo(ip2.getAirLineName());
            if (cmp != 0) {
            	return cmp;
            }
            int m1 = Integer.parseInt(getMonth().toString());
            int m2 = Integer.parseInt(ip2.getMonth().toString());
            return m1 == m2 ? 0 : (m1 < m2 ? -1 : 1);
    }
    
    public int compare(Object object){
    	AirlineTextPair airline2 = (AirlineTextPair) object;    	
    	return getAirLineName().compareTo(airline2.getAirLineName());
    }

    public int hashCode() {
            return this.airLineName.hashCode();
    }

    public boolean equals(Object o) {
            AirlineTextPair p = (AirlineTextPair) o;
            return this.airLineName.equals(p.getAirLineName());
    }
    
    public Text toText(){
    	return new Text("(" + this.airLineName.toString() + "," + this.month.toString() +")");
    }
}
