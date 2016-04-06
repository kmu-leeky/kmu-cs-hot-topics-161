package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.*;

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


public class HashtagCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new HashtagCount(), args);
      
        System.exit(res);
    }

        
	@Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        
        Configuration conf = getConf();
        conf.set("rank", args[2]);

        Job job = Job.getInstance(getConf());
        job.setJarByClass(HashtagCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(KMap.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
      
        return 0;
    }
   
    public static class KMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable ONE = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            for (String token: value.toString().split("\\s+")) {
            	if (token.startsWith("#")) {
            		word.set(token.toLowerCase());
            		context.write(word, ONE);
            	}
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    	private Map<Text, IntWritable> countMap = new HashMap<>();
        
    	@Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            //context.write(key, new IntWritable(sum));
            countMap.put(new Text(key), new IntWritable(sum));
        }
    	
    	@Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
    		
    		/**Make a HasMmap in reduce, and sort the HashMap using "sortByValues" in cleanup */
            Map<Text, IntWritable> sortedMap = sortByValues(countMap);   

            int counter = 0;
            int max = context.getConfiguration().getInt("rank", 1);
            for (Text key : sortedMap.keySet()) {
                if (counter++ == max) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }           
    }  
    
    /**source from: https://github.com/andreaiacono/MapReduce/blob/master/src/main/java/samples/topn_enhanced/EnhancedTopN.java */
    
	private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
    }
    
}
