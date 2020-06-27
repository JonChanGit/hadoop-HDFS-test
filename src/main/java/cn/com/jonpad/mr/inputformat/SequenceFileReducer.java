package cn.com.jonpad.mr.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Jon Chan
 * @date 2020/6/27 16:36
 */
public class SequenceFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
  @Override
  protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
    for (BytesWritable value : values) {
      context.write(key, value);
    }
  }
}
