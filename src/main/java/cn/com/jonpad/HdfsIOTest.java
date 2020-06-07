package cn.com.jonpad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Jon Chan
 * @date 2020/6/7 15:20
 */
public class HdfsIOTest {

  /**
   * IO上传
   * @throws IOException
   * @throws InterruptedException
   * @throws URISyntaxException
   */
  @Test
  public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {

    // 1 获取文件系统
    Configuration configuration = new Configuration();
    FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "hadoop");

    // 2 创建输入流（本地）
    FileInputStream fis = new FileInputStream(new File("/Users/jonchan/Downloads/apple_health_export/导出.xml"));

    // 3 获取输出流（HDFS中）
    FSDataOutputStream fos = fs.create(new Path("/user/hadoop/apple_health_export/导出.xml"));

    // 4 流对拷
    IOUtils.copyBytes(fis, fos, configuration);

    // 5 关闭资源
    IOUtils.closeStream(fos);
    IOUtils.closeStream(fis);
    fs.close();
  }

  /**
   * IO下载
   */
  @Test
  public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{

    // 1 获取文件系统
    Configuration configuration = new Configuration();
    FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "atguigu");

    // 2 获取输入流（HDFS中）
    FSDataInputStream fis = fs.open(new Path("/user/hadoop/apple_health_export/导出.xml"));

    // 3 获取输出流（本地）
    FileOutputStream fos = new FileOutputStream(new File("/Users/jonchan/Downloads/xxx.xml"));

    // 4 流的对拷
    IOUtils.copyBytes(fis, fos, configuration);

    // 5 关闭资源
    IOUtils.closeStream(fos);
    IOUtils.closeStream(fis);
    fs.close();
  }


}
