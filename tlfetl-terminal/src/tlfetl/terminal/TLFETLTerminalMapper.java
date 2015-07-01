package tlfetl.terminal;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author maxi
 */
public class TLFETLTerminalMapper extends Mapper<LongWritable, Text, Text, TLFDWHValue> {

    /**
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String fiid_term = line.substring(0, 4);
        String term_id = line.substring(4, 10);
        String tran_dat = "20" + line.substring(45, 47) + "-" + line.substring(47, 49) + "-" + line.substring(49, 51);
        String tran_tim = line.substring(51, 53);
        String typ = line.substring(57, 61);
        String typ_cde = line.substring(64, 66);
        String tran_cde = line.substring(72, 78);
        String tipo_dep = line.substring(97, 98);
        String resp_cde = line.substring(135, 138);
        String term_type = line.substring(138, 140);
        String term_ln = line.substring(156, 160);
        String crncy_cde = line.substring(160, 163);
        float amt_1 = Float.valueOf(line.substring(123, 135)) / 100;

        context.write (new Text(fiid_term + "," + term_id + "," + tran_dat + "," + tran_tim + "," + typ + "," + typ_cde + "," + 
                tran_cde + "," + tipo_dep + "," + resp_cde + "," + term_type + "," + term_ln + "," + crncy_cde + ","),
                new TLFDWHValue(amt_1));
    }
}
