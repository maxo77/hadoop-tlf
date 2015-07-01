/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tlfetl.terminal;

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
public class TLFETLTerminal extends Configured implements Tool {

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TLFETLTerminal(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.default.name", "hdfs://hadooptest:9000");
        conf.set("mapred.job.tracker", "hadooptest:50070");
        Job job = Job.getInstance(conf); 
        job.setJarByClass(TLFETLTerminal.class);
        job.setJobName("TLF General ETL");

        FileInputFormat.addInputPath(job, new Path(conf.get("inputPath")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("outputPath")));

        job.setMapperClass(TLFETLTerminalMapper.class);
        job.setReducerClass(TLFETLTerminalReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TLFDWHValue.class);

        boolean exitCode = job.waitForCompletion(true);
        return exitCode ? 0 : 1;
    }
}
