/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tlfetl.general;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author maxi
 */
public class TlfEtlGeneral extends Configured implements Tool {

    @Override
    public int run(String[] arg0) throws Exception {

        Configuration conf = getConf();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.default.name", "hdfs://hadooptest:9000");
        conf.set("mapred.job.tracker", "hadooptest:50070");
        Job job = Job.getInstance(conf); 
        job.setJarByClass(TlfEtlGeneral.class);
        job.setJobName("TLF General ETL");

        FileInputFormat.addInputPath(job, new Path(conf.get("inputPath")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("outputPath")));

        job.setMapperClass(TLFETLGeneralMapper.class);
        job.setReducerClass(TLFETLGeneralReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TLFDWHValue.class);

        boolean exitCode = job.waitForCompletion(true);
        return exitCode ? 0 : 1;
    }
    
    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TlfEtlGeneral(), args);
        System.exit(exitCode);
    }
}
