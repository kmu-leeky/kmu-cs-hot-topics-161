package kr.ac.kookmin.cs.bigdata;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool{
	
	
	public static void main(String[] args) throws Exception {
		
		System.out.println(Arrays.toString(args));
		if(args.length < 3) System.exit(0);
		
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);
		System.exit(res);
	}
	

	@Override
	public int run(String[] args) throws Exception {
		
		System.out.println(Arrays.toString(args));
		
		Configuration jobconf = getConf();
		jobconf.set("TopK", args[2]);
		Job job = Job.getInstance(jobconf);
		job.setJarByClass(WordCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(TopNMap.class);
		job.setReducerClass(TopNReduce.class);
				
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		return 0;
	}
	
	public static class TopNMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable ONE = new IntWritable(1);
		private Text word = new Text();
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
						
			for(String token : value.toString().split(" +")){
				String []words = token.split("#+");
				
				int i = 0;
				for(String temp : words){
					if(i++ == 0) continue;
					
					if(temp.length() > 0){
						word.set(temp);
						context.write(word, ONE);
					}
				}
				
			}
		}
	}
	
	
	public static class TopNReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		private Map<Text, IntWritable> countMap = new HashMap();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable val : values){
				sum += val.get();
			}
			
			countMap.put(new Text(key),  new IntWritable(sum));
			//context.write(key,  new IntWritable(sum));
		}
		
		@Override
		protected void cleanup(
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			Map<Text, IntWritable> sortedMap = sortByValues(countMap);
			int count = 0;
			int topK = Integer.parseInt(context.getConfiguration().get("TopK"));
			
			for(Text key : sortedMap.keySet()){
				if(count++ == topK) break;
				context.write(key,  sortedMap.get(key));
			}
		}
	}
	
	private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map){
		List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K,V>>(map.entrySet());
		
		Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

			@Override
			public int compare(Entry<K, V> o1, Entry<K, V> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		
		
		Map<K, V> sortedMap = new LinkedHashMap<K, V>();
		
		for(Map.Entry<K, V> entry : entries){	
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		
		return sortedMap;
	}
}

