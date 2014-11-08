package com.neu.cs6240;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.examples.SecondarySort.IntPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * AirlineTextPair implements the WritableComparable
 * This class contains useful functions used for keyComparator and 
 * Grouping comparator
 */
public class AirlineTextPair implements WritableComparable {
	// Airline's UniqueCarrier
    Text airLineName;    
    // Airlines fly Month
    Text month;

    /**
     * constructor
     */
    public AirlineTextPair() {
    	this.airLineName = new Text();
    	this.month = new Text();
    }
    
    /**
     * constructor
     * @param airLineName
     * @param month
     */
    public AirlineTextPair(Text airLineName, Text month) {
    	this.airLineName = airLineName;
    	this.month = month;
    }
   
    /**
     * set airline's unique carrier name
     * @param airLineName
     */
    public void setAirLineName(String airLineName) {
    	this.airLineName.set(airLineName.getBytes());
    }   
   
    /**
     * set the month of the flight fly
     * @param month
     */
    public void setMonth(String month) {
            this.month.set(month.getBytes());
    }

    /**
     * get airline's unique carrier name
     */
    public Text getAirLineName() {
            return this.airLineName;
    }

    /**
     * get the month of the flight fly
     */
    public Text getMonth() {
            return this.month;
    }

    /**
     * overrider the write method to support write operation
     */
    public void write(DataOutput out) throws IOException {
    	this.airLineName.write(out);
    	this.month.write(out);
    }

    /**
     * overrider readFiled method to support reading fields 
     */
    public void readFields(DataInput in) throws IOException {
            if (this.airLineName == null)
            	this.airLineName = new Text();

            if (this.month == null)
            	this.month = new Text();

            this.airLineName.readFields(in);
            this.month.readFields(in);
    }

    /**
     * Sort first by airline name and then by month in increasing order
     */
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
    
    /**
     * provide comparator for airline name for grouping comparator
     */
    public int compare(Object object){
    	AirlineTextPair airline2 = (AirlineTextPair) object;    	
    	return getAirLineName().compareTo(airline2.getAirLineName());
    }

    /**
     * use this hashcode for partitioning
     */
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
