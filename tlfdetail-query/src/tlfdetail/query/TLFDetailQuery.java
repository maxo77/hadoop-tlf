/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tlfdetail.query;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.common.IOUtils;

/**
 *
 * @author maxi
 */
public class TLFDetailQuery extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.default.name", "hdfs://hadooptest:9000");
        conf.set("mapred.job.tracker", "hadooptest:50070");
        Job job = Job.getInstance(conf); 
        job.setJarByClass(TLFDetailQuery.class);
        job.setJobName("TLF Detail Search");

        FileInputFormat.addInputPath(job, new Path(conf.get("inputPath")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("outputPath")));

        job.setMapperClass(TLFDetailMapper.class);
        job.setReducerClass(TLFDetailReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean exitCode = job.waitForCompletion(true);
        if(exitCode == true)
            showResult("/user/tlf/out/part-r-00000", conf);
        return exitCode ? 0 : 1;
    }

    void showResult(String path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(path), conf);
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(path));
            while(in.available() > 0)
                IOUtils.copyBytes(in, System.out, 217, true);
        } finally {
            IOUtils.closeStream(in);
        }
    }
    
    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TLFDetailQuery(), args);
        System.exit(exitCode);
    }
}
