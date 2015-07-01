/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tlfdetail.query;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TLFDetailMapper extends Mapper<LongWritable, Text, Text, Text> {

	TLFDetailSearchFieldList searchFieldList;

	@Override
	public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String seqNum = line.substring(33, 45).trim();

		if(searchFieldList.holdsFor(line))
			context.write(new Text(seqNum), new Text(line));
	}

	@Override
	protected void cleanup(Mapper.Context context)
			throws IOException, InterruptedException {

		searchFieldList.clear();
	}

	@Override
	protected void setup(Mapper.Context context)
			throws IOException, InterruptedException {

		searchFieldList = new TLFDetailSearchFieldList();
		searchFieldList.addFilter("fiid_term", 0, 4);
		searchFieldList.addFilter("term_id", 4, 10);
		searchFieldList.addFilter("fiid_card", 10, 14);
		searchFieldList.addFilter("pan", 14, 33);
		searchFieldList.addFilter("seq_num", 33, 45);
		searchFieldList.addFilter("tran_dat", 45, 51);
		searchFieldList.addFilter("tran_tim", 51, 57);
		searchFieldList.addFilter("typ", 57, 61);
		searchFieldList.addFilter("member", 61, 64);
		searchFieldList.addFilter("typ_cde", 64, 66);
		searchFieldList.addFilter("post_dat", 66, 72);
		searchFieldList.addFilter("tran_cde", 72, 78);
		searchFieldList.addFilter("from_acct", 78, 97);
		searchFieldList.addFilter("tipo_dep", 97, 98);
		searchFieldList.addFilter("to_acct", 98, 117);
		searchFieldList.addFilter("new_data_field", 117, 123);
		searchFieldList.addFilter("amt_1", 123, 135);
		searchFieldList.addFilter("resp_cde", 135, 138);
		searchFieldList.addFilter("term_type", 138, 140);
		searchFieldList.addFilter("tipo_cambio", 140, 148);
		searchFieldList.addFilter("cuota_anio", 148, 153);
		searchFieldList.addFilter("ente_pas", 153, 156);
		searchFieldList.addFilter("term_ln", 156, 160);
		searchFieldList.addFilter("crncy_cde", 160, 163);
		searchFieldList.addFilter("card_type", 163, 165);
		searchFieldList.addFilter("codigo_pais", 165, 168);
		searchFieldList.addFilter("loc_term", 168, 181);
		searchFieldList.addFilter("rubro", 181, 186);
		searchFieldList.addFilter("cvvrc", 186, 187);
		searchFieldList.addFilter("direccion_ip", 187, 199);
		searchFieldList.addFilter("canal", 199, 201);
		searchFieldList.addFilter("producto", 201, 203);
		searchFieldList.addFilter("crd_ln", 203, 207);
		searchFieldList.addFilter("tandem_tlf_filename", 207, 216);
		
		Configuration conf = context.getConfiguration();
		searchFieldList.setupFilters(conf);
	}
}
