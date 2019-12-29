package cn.com.jonpad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.LocalDateTime;

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
        fs.mkdirs(new Path("/test"+ Instant.now().toEpochMilli()));

        // 3 关闭资源
        fs.close();
    }

}
