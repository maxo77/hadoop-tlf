/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tlfetl.general;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author maxi
 */
public class TLFDWHValue implements Writable {
    private int count;
    private float amount;

    public TLFDWHValue(float amount) {
        this.amount = amount;
        count = 1;
    }
    
    public TLFDWHValue() {
        this.amount = 0;
        count = 0;
    }
    
    public void add(TLFDWHValue s) {
        amount += s.amount;
        count++;
    }

    @Override
    public String toString() {
        return String.valueOf(amount) + "," + String.valueOf(count);
    }

    @Override
    public void write(DataOutput d) throws IOException {
        FloatWritable writableAmount = new FloatWritable(amount);
        IntWritable writableCount = new IntWritable(count);
        writableAmount.write(d);
        writableCount.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        FloatWritable writableAmount = new FloatWritable();
        IntWritable writableCount = new IntWritable();
        writableAmount.readFields(di);
        writableCount.readFields(di);
        amount = writableAmount.get();
        count = writableCount.get();
    }
}
