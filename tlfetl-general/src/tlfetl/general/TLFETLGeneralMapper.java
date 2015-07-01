package tlfetl.general;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TLFETLGeneralMapper extends Mapper<LongWritable, Text, Text, TLFDWHValue> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

                String fiid_term = line.substring(0, 4);
                String term_id = line.substring(4, 10);
                String fiid_card = line.substring(10, 14);
                String tran_dat = "20" + line.substring(45, 47) + "-" + line.substring(47, 49) + "-" + line.substring(49, 51);
                String typ = line.substring(57, 61);
                String typ_cde = line.substring(64, 66);
                String post_dat = "20" + line.substring(66, 68) + "-" + line.substring(68, 70) + "-" + line.substring(70, 72);
                String tran_cde = line.substring(72, 78);
                String tipo_dep = line.substring(97, 98);
                String resp_cde = line.substring(135, 138);
                String term_type = line.substring(138, 140);
                String ente_pas = line.substring(153, 156);
                String term_ln = line.substring(156, 160);
                String crncy_cde = line.substring(160, 163);
                String card_type = line.substring(163, 165);
                String canal = line.substring(199, 201);
                String producto = line.substring(201, 203);
                String crd_ln = line.substring(203, 207);
                float amt_1 = Float.valueOf(line.substring(123, 135)) / 100;

                context.write (new Text(fiid_term + "," + term_id + "," + fiid_card + "," + tran_dat + "," + typ + "," + typ_cde + "," + 
                        post_dat + "," + tran_cde + "," + tipo_dep + "," + resp_cde + "," + term_type + "," + ente_pas + "," + 
                        term_ln + "," + crncy_cde + "," + card_type + "," + canal + "," + producto + "," + crd_ln + ","),
                        new TLFDWHValue(amt_1));
	}
}
