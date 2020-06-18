package cn.com.jonpad.mr.wordcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Jon Chan
 * @date 2020/6/14 22:02
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


  Text keyWord = new Text();
  IntWritable countValue = new IntWritable(1);

  private static String[] KEY_WORDS = {"biyiCoreVersion"};

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] lineWords = value.toString().split(" ");
    for (String word : lineWords) {
      for (String keyWord : KEY_WORDS) {
        if (word.contains(keyWord)) {
          this.keyWord.set(keyWord);
          context.write(this.keyWord, countValue);
        }
      }
    }
  }
}
