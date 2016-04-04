package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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

public class HashTagCount extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		
		if (args.length < 3) {
			System.out.println("Please Pass 3 parameters");
			return;
		}
			
		
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new HashTagCount(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));

		Job hashTagCountJob = Job.getInstance(getConf());

		hashTagCountJob.setJarByClass(HashTagCount.class);
		hashTagCountJob.setOutputKeyClass(Text.class); // It's possible for seiralizable String
		hashTagCountJob.setOutputValueClass(IntWritable.class);

		hashTagCountJob.setMapperClass(Map.class);
		hashTagCountJob.setReducerClass(Reduce.class);

		hashTagCountJob.setInputFormatClass(TextInputFormat.class);
		hashTagCountJob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(hashTagCountJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(hashTagCountJob, new Path(args[1]));

		hashTagCountJob.waitForCompletion(true);


		// Job for Topk
		Configuration conf = getConf();
		conf.set("topK", args[2]);
		Job topKJob = Job.getInstance(conf);
		
		
		topKJob.setJarByClass(TopKHashTags.class);
		topKJob.setOutputKeyClass(IntWritable.class); // It's possible for seiralizable String
		topKJob.setOutputValueClass(Text.class);

		topKJob.setMapperClass(TopKHashTags.TopKMapper.class);
		topKJob.setReducerClass(TopKHashTags.TopKReducer.class);
		topKJob.setSortComparatorClass(IntWritableDescendingComparator.class);


		topKJob.setInputFormatClass(TextInputFormat.class);
		topKJob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(topKJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(topKJob, new Path(args[1] + "/topkoutput"));

		topKJob.waitForCompletion(true);

		return 0;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			for (String token: value.toString().split("\\s+")) {
				if (token.startsWith("#")){
					word.set(token.toLowerCase());
					context.write(word, ONE);
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class IntWritableDescendingComparator extends WritableComparator{
		
		protected IntWritableDescendingComparator(){
			super(IntWritable.class, true);
		}
		
		@Override
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable w1, WritableComparable w2) {

			IntWritable integer1 = (IntWritable) w1;
			IntWritable integer2 = (IntWritable) w2;

			return (-1) * integer1.compareTo(integer2);
		}
	}
}

