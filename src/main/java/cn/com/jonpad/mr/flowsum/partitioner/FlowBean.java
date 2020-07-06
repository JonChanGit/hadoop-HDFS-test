package cn.com.jonpad.mr.flowsum.partitioner;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Jon Chan
 * @date 2020/6/18 19:48
 */
public class FlowBean implements Writable {

  private long upFlow;
  private long downFlow;
  private long sumFlow;

  public FlowBean() {
  }

  public FlowBean(long upFlow, long downFlow) {
    this.upFlow = upFlow;
    this.downFlow = downFlow;
    sumFlow = upFlow + downFlow;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(this.upFlow);
    out.writeLong(this.downFlow);
    out.writeLong(this.sumFlow);

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.upFlow = in.readLong();
    this.downFlow = in.readLong();
    this.sumFlow = in.readLong();
  }

  @Override
  public String toString() {
    return "FlowBean{" +
      "upFlow=" + upFlow +
      ", downFlow=" + downFlow +
      ", sumFlow=" + sumFlow +
      '}';
  }

  public long getUpFlow() {
    return upFlow;
  }

  public void setUpFlow(long upFlow) {
    this.upFlow = upFlow;
  }

  public long getDownFlow() {
    return downFlow;
  }

  public void setDownFlow(long downFlow) {
    this.downFlow = downFlow;
  }

  public long getSumFlow() {
    return sumFlow;
  }

  public void setSumFlow(long sumFlow) {
    this.sumFlow = sumFlow;
  }

  public void set(long upFlow, long downFlow) {
    this.upFlow = upFlow;
    this.downFlow = downFlow;
    this.sumFlow = upFlow + downFlow;
  }
}
