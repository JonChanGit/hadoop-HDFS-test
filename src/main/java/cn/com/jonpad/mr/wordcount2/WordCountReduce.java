package cn.com.jonpad.mr.wordcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Jon Chan
 * @date 2020/6/14 22:22
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

  IntWritable value = new IntWritable();

  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int count = 0;
    for (IntWritable value : values) {
      count += value.get();
    }
    value.set(count);
    context.write(key, value);
  }
}
