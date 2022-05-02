package hadoop_maxtemp;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Maxtemp_driver {
	public static void main(String args[]) throws Exception, IOException {
		if(args.length!=2) {
			System.out.println("Invalid Arguments");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		Job job =  new Job(conf , "Maxtemp_driver");
		job.setJarByClass(Maxtemp_driver.class);
		job.setMapperClass(Maxtemp_mapper.class);
		job.setReducerClass(Maxtemp_reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
