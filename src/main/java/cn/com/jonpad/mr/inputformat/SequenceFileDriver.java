package cn.com.jonpad.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @author Jon Chan
 * @date 2020/6/27 16:50
 */
public class SequenceFileDriver {
  public static void main(String[] args) throws Exception {
    // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
    args = new String[] {
      "/Users/jonchan/Downloads/apple_health_export/electrocardiograms",
      "/Users/jonchan/Downloads/apple_health_export/electrocardiograms_output"
    };

    Configuration configuration = new Configuration();
    Job job = Job.getInstance(configuration);

    job.setJarByClass(SequenceFileDriver.class);
    job.setMapperClass(SequenceFileMapper.class);
    job.setReducerClass(SequenceFileReducer.class);

    job.setInputFormatClass(WholeFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(BytesWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // 6 提交job
    boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);

  }
}
