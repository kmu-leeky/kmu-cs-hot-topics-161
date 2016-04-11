package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountUserPerTrack extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new CountUserPerTrack(), args);
      
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));

        Job job = Job.getInstance(getConf());
        job.setJarByClass(CountUserPerTrack.class);
        job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
    
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
      
        return 0;
    }
   
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    	
        private Text word = new Text();
        private Text word2 = new Text();
        
       int count = 0;
        
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
      
        	String[] token = value.toString().split("\\t");
        	
         
       		
       		
        		word.set(token[5]);
        		word2.set(token[0]);
        		context.write(word, word2);
        
     
        }
    } 
 
    public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {
    	
    	     
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
           
        	   HashSet<String> hs = new HashSet<String>();
          	 
        	for (Text val : values) {
            	
            	hs.add(val.toString());
            	
            }
            
            
            
            context.write(key, new IntWritable(hs.size()));
        }
    }
}