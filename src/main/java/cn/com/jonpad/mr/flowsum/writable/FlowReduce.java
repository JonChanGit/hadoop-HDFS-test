package cn.com.jonpad.mr.flowsum.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Jon Chan
 * @date 2020/6/18 23:41
 */
public class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean> {

  FlowBean v = new FlowBean();

  @Override
  protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

    long sumUp = 0;
    long sumDown = 0;

    for (FlowBean value : values) {
      sumUp += value.getUpFlow();
      sumDown += value.getDownFlow();
    }

    v.set(sumUp, sumDown);

    context.write(key, v);
  }
}
