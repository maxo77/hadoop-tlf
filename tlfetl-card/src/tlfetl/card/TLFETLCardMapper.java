/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tlfetl.card;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author maxi
 */
class TLFETLCardMapper extends Mapper<LongWritable, Text, Text, tlfetl.card.TLFDWHValue> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String fiid_card = line.substring(10, 14);
        String pan = line.substring(14, 33).trim();
        String tran_dat = "20" + line.substring(45, 47) + "-" + line.substring(47, 49) + "-" + line.substring(49, 51);
        String typ = line.substring(57, 61);
        String typ_cde = line.substring(64, 66);
        String post_dat = "20" + line.substring(66, 68) + "-" + line.substring(68, 70) + "-" + line.substring(70, 72);
        String tran_cde = line.substring(72, 78);
        String tipo_dep = line.substring(97, 98);
        String resp_cde = line.substring(135, 138);
        String ente_pas = line.substring(153, 156);
        String crncy_cde = line.substring(160, 163);
        String card_type = line.substring(163, 165);
        String canal = line.substring(199, 201);
        String producto = line.substring(201, 203);
        String crd_ln = line.substring(203, 207);
        float amt_1 = Float.valueOf(line.substring(123, 135)) / 100;

        context.write (new Text(fiid_card + "," + pan + "," + tran_dat + "," + typ + "," + typ_cde + "," + post_dat + "," + 
                tran_cde + "," + tipo_dep + "," + resp_cde + "," + ente_pas + "," + crncy_cde + "," + card_type + "," + 
                canal + "," + producto + "," + crd_ln + ","),
                new tlfetl.card.TLFDWHValue(amt_1));
    }
}
