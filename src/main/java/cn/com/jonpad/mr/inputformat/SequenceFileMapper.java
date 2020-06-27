package cn.com.jonpad.mr.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Jon Chan
 * @date 2020/6/27 16:34
 */
public class SequenceFileMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {
  @Override
  protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
    context.write(key, value);
  }
}
