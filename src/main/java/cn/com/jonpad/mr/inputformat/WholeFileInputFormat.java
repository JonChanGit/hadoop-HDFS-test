package cn.com.jonpad.mr.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author Jon Chan
 * @date 2020/6/27 15:25
 */
public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {

  /**
   * 文件是否可分割
   * @param context
   * @param filename
   * @return
   */
  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    WholeRecordReader recordReader = new WholeRecordReader();
    recordReader.initialize(split, context);

    return recordReader;
  }
}
