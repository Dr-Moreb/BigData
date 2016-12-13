import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class RecommenderSystem{
	public static class RecMap extends Mapper<Object, Text, Text, Text>{
		private Text item = new Text();
		private Text boughtWith = new Text();

		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] st = line.split(",");
			for(String s1 : st){
				for(String s2: st){
					if(!s2.equals(s1)){
						item.set(s1);
						boughtWith.set(s2);
						context.write(item, boughtWith);
					}
				}
				
			}

		}
	}

	public static class RecReduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{
			HashSet<Text> hset = new HashSet<Text>();
			for(Text val: values){
				String line = val.toString();
				String[] st = line.split(",");
				for(int i = 0; i< st.length;i++){
					if(!hset.contains(st[i])){
						hset.add(new Text(st[i]));
					}		
				}
			}
			context.write(key, new Text(hset.toString()));
		
//			HashSet<Text> hs = new HashSet<Text>();
//			for(Text val: values){
//				if(!hs.contains(val)) hs.add(val);
//			}
//			context.write(key, new Text(hs.toString()));
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();

		Job job = new Job(conf, "RecommenderSystemjob");
		job.setJarByClass(RecommenderSystem.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(RecMap.class);
		job.setReducerClass(RecReduce.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}

