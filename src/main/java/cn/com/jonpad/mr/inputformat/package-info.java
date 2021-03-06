/**
 * 无论HDFS还是MapReduce，
 * 在处理小文件时效率都非常低，
 * 但又难免面临处理大量小文件的场景，
 * 此时，就需要有相应解决方案。
 * 可以自定义InputFormat实现小文件的合并。
 *
 * 将多个小文件合并成一个SequenceFile文件
 * （SequenceFile文件是Hadoop用来存储二进制形式的key-value对的文件格式），
 * SequenceFile里面存储着多个文件，
 * 存储的形式为文件路径+名称为key，
 * 文件内容为value。
 * @author Jon Chan
 * @date 2020/6/27 17:25
 */
package cn.com.jonpad.mr.inputformat;
