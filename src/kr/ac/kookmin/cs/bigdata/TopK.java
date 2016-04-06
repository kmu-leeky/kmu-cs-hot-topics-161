package topk;

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

public class TopK extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		       
    	System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new TopK(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
    	
        System.out.println(Arrays.toString(args));

        Configuration conf = getConf();
       	conf.set("rank", args[2]);
        
        Job job = Job.getInstance(getConf());
        job.setJarByClass(TopK.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TopKMap.class);
        job.setReducerClass(TopKReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
        return 0;
    }


    public static class TopKMap extends Mapper<LongWritable, Text, Text, IntWritable> {
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

    public static class TopKReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    	
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
        protected void cleanup(Context context) throws IOException, InterruptedException {

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
        
        private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
            List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

            Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

                @Override
                public int compare(Map.Entry<K, V> topk_1, Map.Entry<K, V> topk_2) {
                    return topk_2.getValue().compareTo(topk_1.getValue());
                }
            });

            Map<K, V> sortedMap = new LinkedHashMap<K, V>();

            for (Map.Entry<K, V> entry : entries) {
                sortedMap.put(entry.getKey(), entry.getValue());
            }

            return sortedMap;
            }
    }
