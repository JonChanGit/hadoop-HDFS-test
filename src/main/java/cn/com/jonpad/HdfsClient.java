package cn.com.jonpad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;

/**
 * @author Jon Chan
 * @date 2019/12/30 00:08
 */
public class HdfsClient {
  @Test
  public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {

    // 1 获取文件系统
    Configuration configuration = new Configuration();
    // 配置在集群上运行 方案1 需要设置原型JVM变量 [ -DHADOOP_USER_NAME=hadoop ]
    // configuration.set("fs.defaultFS", "hdfs://hadoop1:9000");
    // FileSystem fs = FileSystem.get(configuration);

    //  配置在集群上运行 方案2
    FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "hadoop");

    // 2 创建目录
    fs.mkdirs(new Path("/test" + Instant.now().toEpochMilli()));

    // 3 关闭资源
    fs.close();
  }


  /**
   * 文件上传测试
   * 添加配置文件,限制只有一个副本
   * <p>参数优先级</p>
   * <p>
   * 参数优先级排序：（1）客户端代码中设置的值 >（2）ClassPath下的用户自定义配置文件 >（3）然后是服务器的默认配置
   * </p>
   */
  @Test
  public void testUploadFile() throws Exception {
    // 1 获取FS对象
    FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop1:9000"), new Configuration(), "hadoop");

    // 2 执行上传API
    fileSystem.copyFromLocalFile(new Path("/Users/jonchan/Downloads/staff.sql"), new Path("/user/hadoop/staff_only_one_copy.sql"));

    // 3 关闭资源
    fileSystem.close();
  }

  /**
   * 下载
   * @throws Exception
   */
  @Test
  public void testDownload() throws Exception {
    // 1 获取FS对象
    FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop1:9000"), new Configuration(), "hadoop");

    // 2 执行下载API(删除源文件,HDFS路径,本地路径,是否文件校验)
    fileSystem.copyToLocalFile(
      true,
      new Path("/user/hadoop/staff_only_one_copy.sql"),
      new Path("/Users/jonchan/Downloads/staff_tmp.sql"),
      true);

    // 3 关闭资源
    fileSystem.close();
  }


  /**
   * 显示所有文件详情详情
   * @throws Exception
   */
  @Test
  public void testShowDetails() throws Exception {
    // 1 获取FS对象
    FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop1:9000"), new Configuration(), "hadoop");

    // 2 执行API 递归信息
    RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);

    while (listFiles.hasNext()){
      LocatedFileStatus status = listFiles.next();

      System.out.println("文件名: " + status.getPath().toString());
      System.out.println("长度: " + status.getLen());
      System.out.println("权限: " + status.getPermission());
      System.out.println("分组: " + status.getGroup());
      System.out.println("类型: " + (status.isFile()?"文件":"文件夹"));
      // 获取存储的块信息
      BlockLocation[] blockLocations = status.getBlockLocations();

      for (BlockLocation blockLocation : blockLocations) {

        System.out.print("块存储主机节点: " );
        // 获取块存储的主机节点
        String[] hosts = blockLocation.getHosts();
        for (String host : hosts) {
          System.out.print(host+";");
        }
        System.out.println();
      }
      System.out.println("======================");

    }

    // 3 关闭资源
    fileSystem.close();
  }



}
