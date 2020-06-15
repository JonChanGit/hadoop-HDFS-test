package cn.com.jonpad.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author Jon Chan
 * @date 2020/6/14 22:02
  */
public class WordCountMapper extends Mapper<LongWritable, Text, Text,  IntWritable> {


  Text keyWord = new Text();
  IntWritable countValue = new IntWritable(1);

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] lineWords = value.toString().split(" ");
    for (String word : lineWords) {
      keyWord.set(word);

      context.write(keyWord, countValue);
    }
  }
}
