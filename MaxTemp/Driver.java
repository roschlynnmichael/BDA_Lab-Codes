package hadoop_maxtemp;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Maxtemp_driver{
	public static void main(String args[]) throws IOException{
		if(args.length!=2) {
			System.out.println("Invalid Args");
			System.exit(-1);
		}
		JobConf conf = new JobConf(Maxtemp_driver.class);
		conf.setJobName("Finding Max Temp");
		conf.setMapperClass(Maxtemp_mapper.class);
		conf.setReducerClass(Maxtemp_reducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		conf.setNumMapTasks(5);
		conf.setNumReduceTasks(5);
		JobClient.runJob(conf);
	}
}
