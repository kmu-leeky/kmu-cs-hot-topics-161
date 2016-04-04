package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKHashTags extends Configured {

	public static class TopKMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		Text hashTag;
		IntWritable count;
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			for (String token: value.toString().split("\\s+")) {
				if (isNumeric(token))
					count = new IntWritable(Integer.parseInt(token.trim()));
				else
					hashTag = new Text(token.trim());
			}
			
			context.write(count, hashTag);
		}
	}
	
	public static class TopKReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

		Configuration conf;
		int topK;
		static int nr_topKHashTags = 0;
		
		@Override
		protected void setup(
				Reducer<IntWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			conf = context.getConfiguration();
			topK= Integer.parseInt(conf.get("topK"));
		}

		@Override
		protected void reduce(IntWritable intvalue, Iterable<Text> hashtags, Context context)
				throws IOException, InterruptedException {
			
			if (nr_topKHashTags >= topK)
				return;
			for (Text token: hashtags) {
				context.write(new Text(token), intvalue);
				nr_topKHashTags++;
			}
		}
	}

	public static boolean isNumeric(String str)
	{
		try
		{
			Double.parseDouble(str);
		}
		catch(NumberFormatException nfe)
		{
			return false;
		}
		return true;
	}
}
