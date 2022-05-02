package hadoop_wordcount;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WC_Driver {
	public static void main(String args[]) throws IOException{
		if(args.length!=2) {
			System.out.println("Invalid Arguments Passed");
			System.exit(-1);
		}
		JobConf conf=new JobConf(WC_Driver.class);
		conf.setMapperClass(WC_Mapper.class);
		conf.setReducerClass(WC_Reducer.class);
		conf.setCombinerClass(WC_Reducer.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		JobClient.runJob(conf);
		
		
	}
}

