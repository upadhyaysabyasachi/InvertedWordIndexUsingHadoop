package com.xml;



import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex {

                                
                public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

                                private static final String EOF = null;
								private Text word = new Text();

                                public void map(LongWritable key, Text value, Context context)
                                                                throws IOException, InterruptedException {
                                				
                                				if (value.toString()!=null)
                                					{
                                				
                                                String[] record = value.toString().split("\\*\\*\\^\\*\\*");
                                                
                                                
                                                //Extracting urls,filenames,filetype,users
                                                
                                                String url=record[0];    
                                                String filetype=record[1];
                                                String filename=record[2];
                                                String users=record[3];
                                                
                                                String tagWeightPairs;
                                                StringTokenizer token=new StringTokenizer(record[4]);
                                                
                                                while(token.hasMoreTokens()){
                                                       tagWeightPairs=token.nextToken();
                                                
                                                       String tag=tagWeightPairs.split("==")[0];
                                                       String weight=tagWeightPairs.split("==")[1];
                                                       word.set(new Text(tag));
                                                       
                                                       context.write(word, new Text(filename));
                                                       
                                                       
                                                }
                                               
                                                
                                					}  

                                                
                                                
                                                

                                }
                }
                                
                                public static class SortingPartitioner extends Partitioner<Text, Text>{
                                                
                                                public int getPartition(Text key, Text value, int numPartitions)
                                                {
                                                                if(key.toString().toUpperCase().charAt(0)>='A' && key.toString().toUpperCase().charAt(0)<='Z' )
                                                                return key.toString().toUpperCase().charAt(0) - 'A';
                                                                else
                                                                                return 26;
                                                                                                                                                
                                                }
                } //map ends
                

                public static class MyReducer extends Reducer<Text, Text, Text, Text> {
                     public void reduce(Text key, Iterable<Text> values, Context context)
                            throws IOException, InterruptedException {
                           
                           
                           String list="";
                           for(Text val: values){
                                  list+=val+",";
                           
                           }
                           context.write(key,new Text(list));
                     }
                     }
                

                public static void main(String[] args) throws Exception {

                                Configuration conf = new Configuration();

                                String[] otherArgs = new GenericOptionsParser(conf, args)
                                                                .getRemainingArgs();

                                Job job = new Job(conf, "Word Counter");

                                job.setJarByClass(InvertedIndex.class);
                                job.setMapperClass(MyMapper.class);
                                job.setReducerClass(MyReducer.class);

                                job.setMapOutputKeyClass(Text.class);
                                job.setMapOutputValueClass(Text.class);

                                job.setOutputKeyClass(Text.class);
                                job.setOutputValueClass(Text.class);
                                
                                job.setPartitionerClass(SortingPartitioner.class);
                                    job.setNumReduceTasks(27);

                                FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
                                FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

                                System.exit(job.waitForCompletion(true) ? 0 : 1);
                }
}


