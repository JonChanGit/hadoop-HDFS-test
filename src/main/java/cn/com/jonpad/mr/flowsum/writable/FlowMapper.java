package cn.com.jonpad.mr.flowsum.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Jon Chan
 * @date 2020/6/18 21:37
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

  Text k = new Text();
  FlowBean v = new FlowBean();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] items = value.toString().split("\t");

    String phone = items[1];
    k.set(phone);

    v.setDownFlow(Long.parseLong(items[4]));
    v.setUpFlow(Long.parseLong(items[5]));

    context.write(k, v);

  }
}
