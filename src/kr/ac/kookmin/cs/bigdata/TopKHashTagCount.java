package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.Arrays;

import kr.ac.kookmin.cs.bigdata.HashTagCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopKHashTagCount extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new TopKHashTagCount(),
				args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));

		Job job1_hashCount = Job.getInstance(getConf());
		job1_hashCount.setJarByClass(HashTagCount.class);
		job1_hashCount.setOutputKeyClass(Text.class);
		job1_hashCount.setOutputValueClass(IntWritable.class);

		job1_hashCount.setMapperClass(HashTagCount.Map.class);
		job1_hashCount.setReducerClass(HashTagCount.Reduce.class);

		job1_hashCount.setInputFormatClass(TextInputFormat.class);
		job1_hashCount.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job1_hashCount, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1_hashCount, new Path(args[1]));

		job1_hashCount.waitForCompletion(true);

		Configuration conf = getConf();
		conf.set("limitTopK", args[2]);

		Job job2_topK = Job.getInstance(conf);
		job2_topK.setJarByClass(TopKHashTagCount.class);
		job2_topK.setOutputKeyClass(IntWritable.class);
		job2_topK.setOutputValueClass(Text.class);

		job2_topK.setMapperClass(TopKHashTagCount.Map.class);

		job2_topK.setCombinerClass(TopKHashTagCount.Combine.class);
		// job2_topK.setCombinerKeyGroupingComparatorClass(IntWritableDescendingComparator.class);
		
		job2_topK.setReducerClass(TopKHashTagCount.Reduce.class);
		job2_topK.setSortComparatorClass(IntWritableDescendingComparator.class);

		job2_topK.setInputFormatClass(SequenceFileInputFormat.class);
		job2_topK.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2_topK, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2_topK, new Path(args[1]
				+ "/topkoutput"));

		job2_topK.waitForCompletion(true);
		return 0;
	}

	public static class Map extends
			Mapper<Text, IntWritable, IntWritable, Text> {

		@Override
		public void map(Text hashtag, IntWritable count, Context context)
				throws IOException, InterruptedException {

			context.write(count, new Text(hashtag));
		}
	}

	public static class Combine extends
			Reducer<IntWritable, Text, IntWritable, Text> {

		Configuration conf;
		int limitTopK;
		int countTopK;

		@Override
		protected void setup(
				Reducer<IntWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			conf = context.getConfiguration();
			limitTopK = Integer.parseInt(conf.get("limitTopK"));
			countTopK = 0;
		}

		@Override
		protected void reduce(IntWritable count, Iterable<Text> hashtags,
				Context context) throws IOException, InterruptedException {

			for (Text hashtag : hashtags) {
				if (countTopK >= limitTopK)
					return;

				context.write(count, hashtag);
				countTopK++;
			}
		}
	}

	public static class Reduce extends
			Reducer<IntWritable, Text, Text, IntWritable> {

		Configuration conf;
		int limitTopK;
		int countTopK;

		@Override
		protected void setup(
				Reducer<IntWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			conf = context.getConfiguration();
			limitTopK = Integer.parseInt(conf.get("limitTopK"));
			countTopK = 0;
		}

		@Override
		protected void reduce(IntWritable count, Iterable<Text> hashtags,
				Context context) throws IOException, InterruptedException {

			for (Text hashtag : hashtags) {
				if (countTopK >= limitTopK)
					return;

				context.write(hashtag, count);
				countTopK++;
			}
		}
	}

	public static class IntWritableDescendingComparator extends WritableComparator {
		public IntWritableDescendingComparator() {
			super(IntWritable.class, true);
		}

		@Override
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable w1, WritableComparable w2) {

			IntWritable i1 = (IntWritable) w1;
			IntWritable i2 = (IntWritable) w2;

			return -1 * i1.compareTo(i2);
		}
	}
}