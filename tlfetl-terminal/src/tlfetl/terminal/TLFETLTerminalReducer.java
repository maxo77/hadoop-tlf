/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tlfetl.terminal;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author maxi
 */
public class TLFETLTerminalReducer  extends Reducer<Text, TLFDWHValue, Text, TLFDWHValue> {
    
    @Override public void reduce(Text key, Iterable<TLFDWHValue> values, Context context) throws IOException, InterruptedException {

        TLFDWHValue summarization = new TLFDWHValue();
        for (TLFDWHValue value : values)
            summarization.add(value);
        context.write(key, summarization);
    }
}
