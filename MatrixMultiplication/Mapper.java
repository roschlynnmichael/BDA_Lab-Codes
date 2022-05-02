package hadoop_matrixmultiplication;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class MM_Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		Configuration conf = new Configuration();
		int man = Integer.parseInt(conf.get("m"));
		int pan = Integer.parseInt(conf.get("p"));
		String line = value.toString();
		String[] indices = line.split(",");
		Text outputkey = new Text();
		Text outputvalue = new Text();
		if(indices[0].equals("M")) {
			for(int k=0;k<pan;k++) {
				outputkey.set(indices[1] + "," + k);
				outputvalue.set(indices[0] + "," + indices[2] + "," + indices[3]);
				context.write(outputkey, outputvalue);
			}
		}
		else {
			for(int i=0;i<man;i++) {
				outputkey.set(i + "," + indices[2]);
				outputvalue.set("N," + indices[1] + "," + indices[3]);
				context.write(outputkey, outputvalue);
			}
		}
	}
}
