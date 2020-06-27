package cn.com.jonpad.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author Jon Chan
 * @date 2020/6/27 15:56
 */
public class WholeRecordReader extends RecordReader<Text, BytesWritable> {
  FileSplit split;
  Configuration configuration;

  private long start;
  private long length;
  private long end;

  private boolean progress = true;
  private BytesWritable value = new BytesWritable();
  private Text k = new Text();

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    this.split = (FileSplit) split;
    this.configuration = context.getConfiguration();
    start = this.split.getStart();
    end = start + split.getLength();
    length = split.getLength();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if(progress){
      byte[] contents = new byte[(int) length];
      Path path = split.getPath();
      FileSystem fs = path.getFileSystem(configuration);

      FSDataInputStream fsi = fs.open(path);

      IOUtils.readFully(fsi, contents, 0, contents.length);

      value.set(contents, 0, contents.length);
      String name = split.getPath().toString();
      k.set(name);

      progress = false;

      return  true;
    }
    return  false;
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return k;
  }

  @Override
  public BytesWritable getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {

  }
}
