package cn.com.jonpad.mr.flowsum.partitioner;

import cn.com.jonpad.utils.Assert;
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

    System.out.println(value.toString());

    String downFlow = items[4];
    if (!Assert.isEmpty(downFlow)) {
      v.setDownFlow(Long.parseLong(downFlow));
    }
    String upFlow = items[5];
    if (!Assert.isEmpty(upFlow)) {
      v.setUpFlow(Long.parseLong(upFlow));
    }

    context.write(k, v);

  }
}
