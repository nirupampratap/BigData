package edu.gatech.cse6242;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q4 {

  public static class FirstMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable mOne = new IntWritable(-1);
    private Text source = new Text();
    private Text target = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());

      String src = itr.nextToken();
      String tgt = itr.nextToken();

      source.set(src);
      target.set(tgt);
      context.write(source, one);
      context.write(target, mOne);
    }
  }

  public static class GroupByReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int count = 0;

      for (IntWritable val : values) {
	count += val.get();
      }
      result.set(count);
      context.write(key, result);
    }
  }

  public static class SecondMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text source = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());

      String src = itr.nextToken();
      String tgt = itr.nextToken();

      source.set(tgt);
      context.write(source, one);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    /* First - Level - MapReduce */
    Job job = Job.getInstance(conf, "Q4_Job_1");
    job.setJarByClass(Q4.class);
    job.setMapperClass(FirstMapper.class);
    job.setCombinerClass(GroupByReducer.class);
    job.setReducerClass(GroupByReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]+"/temp"));
    
    boolean success = job.waitForCompletion(true);

    /* Second - Level - MapReduce */
    if (success) {
         Job job2 = Job.getInstance(conf, "Q4_Job_2");
         job2.setJarByClass(Q4.class);
         job2.setMapperClass(SecondMapper.class);
         job2.setReducerClass(GroupByReducer.class);
         job2.setCombinerClass(GroupByReducer.class);
         job2.setOutputKeyClass(Text.class);
         job2.setOutputValueClass(IntWritable.class);
         FileInputFormat.addInputPath(job2, new Path(args[1]+"/temp"));
         FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/final"));
         success = job2.waitForCompletion(true);
         System.exit(job2.waitForCompletion(true) ? 0 : 1);
     }
     else {
         System.exit(1);
     }
  }
}
