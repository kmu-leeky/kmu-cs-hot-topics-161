package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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

public class KHashTagCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new KHashTagCount(), args);
      
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        
        Configuration conf = getConf();
        conf.set("topK",  args[2]);

        Job job = Job.getInstance(getConf());
        job.setJarByClass(KHashTagCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(KMap.class);
        job.setReducerClass(KReduce.class);

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

    public static class KReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    	
    	private Map<Text, Integer> countMap = new HashMap<>();
    	
    	@Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            countMap.put(new Text(key), new Integer(sum));
        }
    	
    	protected void cleanup(Context context)
    			throws IOException, InterruptedException {
            int K = context.getConfiguration().getInt("topK", 10);
    		
    		ValueComparator vc = new ValueComparator(countMap);
			TreeMap<Text, Integer> tmap = new TreeMap<Text, Integer>(vc);
			
			tmap.putAll(countMap);
			
			int count = 0;
			
			for (Map.Entry<Text, Integer> e : tmap.entrySet()) {
				if (count++ == K) {
					break;
				}
				Text key = e.getKey();
				int value = countMap.get(e.getKey());
				context.write(new Text(key), new IntWritable(value));
			}
    	}
    }
}

class ValueComparator implements Comparator<Text> {
	
	private Map<Text, Integer> base;
	
	public ValueComparator(Map<Text, Integer> base) {
		this.base = base;
	}
	
	public int compare(Text a, Text b) {
		if (base.get(a) >= base.get(b)) {
			return -1;
		} else {
			return 1;
		}
	}
}