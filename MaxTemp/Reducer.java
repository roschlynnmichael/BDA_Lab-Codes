package hadoop_maxtemp;

import java.util.Iterator;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Maxtemp_reducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
	public void reduce(Text key, Iterator<IntWritable> value1, OutputCollector<Text, IntWritable> values, Reporter r) throws IOException{
		int maxvalue=Integer.MIN_VALUE;
		while(value1.hasNext()) {
			maxvalue=Math.max(maxvalue, value1.next().get());
		}
		values.collect(key, new IntWritable(maxvalue));
	}
}
