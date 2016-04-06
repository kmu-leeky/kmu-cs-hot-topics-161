package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HashTagCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
            System.out.println(Arrays.toString(args));
	            int res = ToolRunner.run(new Configuration(), new HashTagCount(), args);
		          
			          System.exit(res);
				      }

				          @Override
					      public int run(String[] args) throws Exception {
					              System.out.println(Arrays.toString(args));
						              
							              //save the argument 3
								              Configuration conf = new Configuration();
									              conf.set("number", args[2]);

										              Job job = Job.getInstance(conf);
											              job.setJarByClass(HashTagCount.class);
												              job.setOutputKeyClass(Text.class);
													              job.setOutputValueClass(IntWritable.class);

														              job.setMapperClass(Map.class);
															              job.setReducerClass(Reduce.class);

																              job.setInputFormatClass(TextInputFormat.class);
																	              job.setOutputFormatClass(TextOutputFormat.class);

																		              FileInputFormat.addInputPath(job, new Path(args[0]));
																			              FileOutputFormat.setOutputPath(job, new Path(args[1]));

																				              job.waitForCompletion(true);
																					            
																						            return 0;
																							        }
																								   
																								       public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
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
																																													        	
																																															    	private java.util.Map<Text, IntWritable> countMap = new HashMap<>();
																																																    	
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
																																																																		                
																																																																				        	//sort the key by value
																																																																						        	java.util.Map<Text, IntWritable> sortedMap = sortValue(countMap);
																																																																								        	
																																																																										        	//get the arg3 from context
																																																																												            Configuration conf = context.getConfiguration();
																																																																													                int number = Integer.parseInt(conf.get("number"));            

																																																																															            int cnt = 0;
																																																																																                for (Text key : sortedMap.keySet()) {
																																																																																		            	//cnt == number, stop the writing
																																																																																				                if (cnt++ == number) 
																																																																																						                    break;                
																																																																																								                    context.write(key, sortedMap.get(key));
																																																																																										                }
																																																																																												        }
																																																																																													         
																																																																																														         private static java.util.Map<Text, IntWritable> sortValue(java.util.Map<Text, IntWritable> map){
																																																																																															         	List<java.util.Map.Entry<Text, IntWritable>> list = new LinkedList<java.util.Map.Entry<Text, IntWritable>>(map.entrySet());
																																																																																																	        	
																																																																																																			        	//sort by value
																																																																																																					        	java.util.Collections.sort(list, new Comparator<java.util.Map.Entry<Text, IntWritable>>(){
																																																																																																											@Override
																																																																																																															public int compare(Entry<Text, IntWritable> o1,
																																																																																																																					Entry<Text, IntWritable> o2) {
																																																																																																																										// TODO Auto-generated method stub
																																																																																																																															return o2.getValue().compareTo(o1.getValue());
																																																																																																																																			}
																																																																																																																																			        		
																																																																																																																																						        	});
																																																																																																																																								        	
																																																																																																																																										        	
																																																																																																																																												        	java.util.Map<Text, IntWritable> sortMap = new java.util.LinkedHashMap<Text, IntWritable>();
																																																																																																																																														        	for(java.util.Map.Entry<Text, IntWritable> unsortList : list)
																																																																																																																																																        		sortMap.put(unsortList.getKey(), unsortList.getValue());
																																																																																																																																																			        	
																																																																																																																																																								return sortMap;
																																																																																																																																																								        	
																																																																																																																																																										        }
																																																																																																																																																											    }
																																																																																																																																																											    }

