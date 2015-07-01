/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tlfetl.card;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author maxi
 */
class TLFETLCardReducer extends Reducer<Text, tlfetl.card.TLFDWHValue, Text, tlfetl.card.TLFDWHValue> {

    @Override public void reduce(Text key, Iterable<tlfetl.card.TLFDWHValue> values, Context context) throws IOException, InterruptedException {

        tlfetl.card.TLFDWHValue summarization = new tlfetl.card.TLFDWHValue();
        for (tlfetl.card.TLFDWHValue value : values)
            summarization.add(value);

        context.write(key, summarization);
    }
}
