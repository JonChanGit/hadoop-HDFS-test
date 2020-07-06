package cn.com.jonpad.mr.flowsum.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Jon Chan
 * @date 2020/7/6 23:09
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

  private static Map<String, Integer> PARTITIONER_KEY = new HashMap<>(16);


  /**
   * @param key           是手机号
   * @param value         是流量信息
   * @param numPartitions
   * @return
   */
  @Override
  public int getPartition(Text key, FlowBean value, int numPartitions) {

    // 获取手机号前3位

    String preUNum = key.toString().substring(0, 3);

    Integer partition = PARTITIONER_KEY.get(preUNum);
    if(partition == null){
      // partition 必须从0开始
      partition = PARTITIONER_KEY.size();
      PARTITIONER_KEY.put(preUNum,partition);
    }

    return partition;
  }
}
