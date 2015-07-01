package tlfetl.general;

import java.io.IOException;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class TLFETLGeneralReducer extends Reducer<Text, TLFDWHValue, Text, TLFDWHValue> {

    @Override public void reduce(Text key, Iterable<TLFDWHValue> values, Context context) throws IOException, InterruptedException {

        TLFDWHValue summarization = new TLFDWHValue();
        for (TLFDWHValue value : values)
            summarization.add(value);
        context.write(key, summarization);
    }
}
