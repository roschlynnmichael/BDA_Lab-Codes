package hadoop_matrixmultiplication;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.Configuration;

public class MM_Driver{
	public static void main(String args[]) throws Exception{
		if(args.length!=2) {
			System.out.println("Invalid Arguments");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		conf.set("m", "1000");
		conf.set("n", "100");
		conf.set("p", "1000");
		Job job = new Job(conf , "MM_Driver");
		job.setJobName("Matrix Multiplication");
		job.setMapperClass(MM_Mapper.class);
		job.setReducerClass(MM_Reducer.class);
		job.setJarByClass(MM_Driver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		job.waitForCompletion(true);
		
	}
}
