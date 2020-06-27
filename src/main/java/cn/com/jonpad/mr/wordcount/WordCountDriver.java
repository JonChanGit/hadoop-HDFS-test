package cn.com.jonpad.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Jon Chan
 * @date 2020/6/14 22:32
 */
public class WordCountDriver {

  public static void main(String[] args) throws Exception {
    Configuration configuration = new Configuration();
    Job job = Job.getInstance(configuration);

    job.setJarByClass(WordCountDriver.class);

    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReduce.class);


    // 4 设置map输出
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    // 5 设置最终输出kv类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // 设置小文件切片InputFormat
    job.setInputFormatClass(CombineTextInputFormat.class);
    // 最大切片大小4MB
    CombineTextInputFormat.setMaxInputSplitSize(job,4194304);

    // 6 设置输入和输出路径
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // 7 提交
    boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);

  }
}
