package hadoop_matrixmultiplication;

import java.io.IOException;
import org.apache.hadoop.io.*;
import java.util.HashMap;

public class MM_Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>{
	public void map(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		String[] value;
		HashMap<Integer, Float> hasha = new HashMap<Integer, Float>();
		HashMap<Integer, Float> hashb = new HashMap<Integer, Float>();
		for(Text val : values) {
			value = val.toString().split(",");
			if(value[0].equals("M")) {
				hasha.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
			}
			else {
				hashb.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
			}
		}
		int n = Integer.parseInt(context.getConfiguration().get("n"));
		float result = 0.0f;
		float m_ij;
		float n_jk;
		for(int j=0;j<n;j++) {
			m_ij= hasha.containsKey(j) ? hasha.get(j) : 0.0f;
			n_jk= hashb.containsKey(j) ? hashb.get(j) : 0.0f;
			result += m_ij * n_jk;
		}
		if(result!=0.0f) {
			context.write(null, new Text(key.toString() + "," + Float.toString(result)));
		}
	}

}
