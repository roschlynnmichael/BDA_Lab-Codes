package hadoop_wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WC_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	public void map(LongWritable key, Text value, OutputCollector <Text, IntWritable> output, Reporter r) throws IOException{
		String line = value.toString();
		StringTokenizer token = new StringTokenizer(line);
		while(token.hasMoreTokens()) {
			word.set(token.nextToken());
			output.collect(word, one);
		}
	}

}
