package edu.northeastern.cs6240.hw3_mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.mapreduce.Counters;

import org.apache.hadoop.fs.FileSystem;

import org.apache.log4j.Logger;
/*
 * This codebase is referenced from (https://github.com/louridas/hadoop-shortest-paths/blob/master)
 */
public class Driver extends Configured implements Tool {

    private Logger logger = Logger.getLogger(this.getClass());
    
    static enum MoreIterations {
        numUpdated
    }

    
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        conf.set("source", args[2]);
        long numUpdated = 1;
        int code = 0;
        int numIterations = 1;
        FileSystem hdfs = FileSystem.get(conf);

        while (numUpdated > 0) {
            logger.info("Iteration: " + numIterations);
            String input, output;
            Job job = Job.getInstance(conf, "BFS");
            if (numIterations == 1) {
                input = args[0];
            } else {
                input = args[1] + "-" + (numIterations - 1);
            }
            output = args[1] + "-" + numIterations;

            job.setJarByClass(Driver.class);
            job.setMapperClass(SPMapper.class);
            job.setReducerClass(SPReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            code = job.waitForCompletion(true) ? 0 : 1;

            Counters jobCounters = job.getCounters();
            numUpdated = jobCounters.
                findCounter(MoreIterations.numUpdated).getValue();
            if (numIterations > 1) {
                hdfs.delete(new Path(input), true);
                logger.info("Updated: " + numUpdated);
            }
            numIterations += 1;
        }
        return code;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Driver(), args);
        System.exit(exitCode);
    }
}