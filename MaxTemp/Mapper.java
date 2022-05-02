package hadoop_maxtemp;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Maxtemp_mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> values, Reporter r) throws IOException{
		String s = value.toString();
		String s1 = s.substring(15,19);
		int temperature;
		if(s.charAt(87)=='+') {
			temperature = Integer.parseInt(s.substring(88,92));
		}
		else {
			temperature=Integer.parseInt(s.substring(87,92));
		}
		values.collect(new Text(s1), new IntWritable(temperature));
	}
}
