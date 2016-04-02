package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
// Do not import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopKHashTagCount extends Configured implements Tool {
    
	public static void main(String[] args) throws Exception {
        
		System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new TopKHashTagCount(), args);
      
        System.exit(res);
    }
    

    @Override
    public int run(String[] args) throws Exception {
        
    	String tmpPath = "/tmp/tmpResult";
    	
    	System.out.println(Arrays.toString(args));
    	
    	Job job = Job.getInstance(getConf());
        job.setJarByClass(HashTagCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(HashTagCount.Map.class);
        job.setReducerClass(HashTagCount.Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(tmpPath));
    	
        job.waitForCompletion(true);
        
        // Second Map-Reduce
    	
        Job job2 = Job.getInstance(getConf());
        
        job2.setJarByClass(TopKHashTagCount.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setMapperClass(Map.class);
        job2.setReducerClass(Reduce.class);
        job2.setNumReduceTasks(1);

        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(tmpPath+"/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        job2.getConfiguration().setInt("topN", Integer.parseInt(args[2]));
        
        job2.waitForCompletion(true);
      
        return 0;
    }
    
    public static class Map extends Mapper<Text, Text, Text, IntWritable> {
    	
    	Comparator<ItemFreq> comparator = new ItemFreqComparator();
    	PriorityQueue<ItemFreq> queue = new PriorityQueue<ItemFreq>(10, comparator);
    	
    	int topN = 10;

        @Override
		protected void setup(
				Mapper<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			topN = context.getConfiguration().getInt("topN", 10);
		}
        
		@Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
			
			Integer iValue = Integer.parseInt(value.toString());
			
            insert(queue, key.toString(), iValue, topN);
        }

		@Override
		protected void cleanup(
				Mapper<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			// until the Queue's elements is zero
			// take Minimum to print records
			while(queue.size() != 0) {
				ItemFreq item = (ItemFreq)queue.remove();
				context.write(new Text(item.getItem()), new IntWritable(item.getFreq()));
			}
		}
		
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    	
    	Comparator<ItemFreq> comparator = new ItemFreqComparator();
    	PriorityQueue<ItemFreq> queue = new PriorityQueue<ItemFreq>(10, comparator);
    	Stack<ItemFreq> stack = new Stack<ItemFreq>();
    	
    	int topN = 10;
        
        @Override
		protected void setup(
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
        	topN = context.getConfiguration().getInt("topN", 10);
		}

		@Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            // Likewise, insert word and sum
            insert(queue, key.toString(), sum, topN);
        }

		@Override
		protected void cleanup(
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// until the Queue's elements is zero
			// take Minimum to print records
//			while(queue.size() != 0) { // this method is for ascending
//				ItemFreq item = (ItemFreq)queue.remove();
//				context.write(new Text(item.getItem()), new IntWritable(item.getFreq()));
//			}
			while(queue.size() != 0) { // this method is for descending
				ItemFreq item = (ItemFreq)queue.remove();
				stack.push(item);
			}
			while(stack.size() != 0) {
				ItemFreq item = (ItemFreq)stack.pop();
				context.write(new Text(item.getItem()), new IntWritable(item.getFreq()));
			}
			
		}
		
    }
    
    public static void insert(PriorityQueue<ItemFreq> queue, String item, Integer iValue, int topN) {
    	// Find Minimum in PriorityQueue -> Find Maximum in PriorityQueue
    	ItemFreq head = (ItemFreq)queue.peek();
    	
    	// if the number of the Queue's elements < topN
    	// or lValue > Minimum in the Queue
    	if(queue.size() < topN || head.getFreq() < iValue) { // <
    		ItemFreq itemFreq = new ItemFreq(item, iValue);
    		
    		// First add in the Queue
    		queue.add(itemFreq);
    		
    		// if the Queue's elements > topN, delete Minimum
    		if(queue.size() > topN) {
    			queue.remove();
    		}
    	}
    }
    
    public static class ItemFreqComparator implements Comparator<ItemFreq> {
    	
		@Override
		public int compare(ItemFreq x, ItemFreq y) { // < >
			if(x.getFreq() < y.getFreq()) {
				return -1;
			}
			else if(x.getFreq() > y.getFreq()) {
				return 1;
			}
			else return 0;
		}
    	
    }
    
}