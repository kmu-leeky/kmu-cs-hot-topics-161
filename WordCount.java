package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.io.netty.util.collection.IntObjectMap.Entry;
import com.google.common.collect.HashMultiset;

import java.io.IOException;
import java.util.*;

public class WordCount extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);
		System.exit(res);
    }
	
    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        Configuration conf = new Configuration();
        conf.set("topK", args[2]);
        Job job = new Job(conf);
        job.setJobName("Top_K_WordCount");
        
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

	public static class TopNMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);
        private Text word = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            for (String token: value.toString().split("\\s+")) {
            	if(token.startsWith("#")){
            		word.set(token);
            		context.write(word, ONE);
            	}
            }
        }
    }
    public static class TopNReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
       
    	private Map<Text, IntWritable> countMap = new HashMap<>();
    	
    	@Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            
        	int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            countMap.put(new Text(key), new IntWritable(sum));
        }
    	@Override
    	protected void cleanup(Context context) throws IOException, InterruptedException{
    		Map<Text, IntWritable> sortedMap = sortByValues(countMap);
    		int counter = 0;
    		int topKNumber = Integer.parseInt(context.getConfiguration().get("topK"));
    		for (Text key : sortedMap.keySet()) {
    			if (counter++ == topKNumber) break;
              
    			context.write(key, sortedMap.get(key));
            }
    	}
    	private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
            List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());
            Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
                @Override
                public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });
            Map<K, V> sortedMap = new LinkedHashMap<K, V>();

            for (Map.Entry<K, V> entry : entries) {
                sortedMap.put(entry.getKey(), entry.getValue());
            }
            return sortedMap;
        }
    }
}

