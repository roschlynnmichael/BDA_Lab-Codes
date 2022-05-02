package hadoop_maxtemp;

import java.io.IOException;
import org.apache.hadoop.io.*;

public class Maxtemp_reducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>{
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int maxvalue = Integer.MIN_VALUE;
		for(IntWritable value : values) {
			maxvalue = Math.max(maxvalue, value.get());
		}
		context.write(key, new IntWritable(maxvalue));
	}
}
