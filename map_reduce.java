package com.xml;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class map_reduce {
	public static class MyMapper extends
	Mapper<LongWritable, Text, CompositeKey, Text> {

private Text word = new Text();

public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

	
	CompositeKey temp= new CompositeKey();
	String url=value.toString().split("\\*\\*\\^\\*\\*")[0];
	String users=value.toString().split("\\*\\*\\^\\*\\*")[3];
	String tagweightpair= value.toString().split("\\*\\*\\^\\*\\*")[4];
	
	String tag="";
	String weight="";
	
	StringTokenizer pair=new StringTokenizer(tagweightpair);
	
	while(pair.hasMoreTokens())
	{
		
		String token=pair.nextToken();
		tag=token.toString().split("==")[0];
		System.out.println(tag);
		
		weight=token.toString().split("==")[1];
		char c;
		c=tag.toString().charAt(0);
		if((c>='a' && c<='z') ||(c>='A' || c<='Z')  )
		 {
			temp.settag(tag+"\t"+url);
		System.out.println("temp:"+temp.gettag());
		
		temp.setweight(Double.parseDouble(weight)*Double.parseDouble(users) );
		context.write(temp, new Text("LAX") );
		 }
	}
	
	
	
	
	

	

}
}
	
	
	public static class MyReducer extends
	Reducer<CompositeKey, Text, Text, Text> {
public void reduce(CompositeKey key, Iterable<Text> values,
		Context context) throws IOException, InterruptedException {
	int count=0;
	for(Text val : values)
	{
		
		context.write(new Text(key.gettag()+","+key.getweight()), new Text(""));
		
		
		count++;
	}
	
}
}

public static void main(String[] args) throws Exception {

Configuration conf = new Configuration();
 conf.set("mapred.textoutputformat.separator", "," );
String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();

//	Logger log  = Logger.getLogger("sds");
Job job = new Job(conf, "Max ");

job.setMapOutputKeyClass(CompositeKey.class);

job.setPartitionerClass(ActualKeyPartitioner.class);
job.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
job.setSortComparatorClass(CompositeKeyComparator.class);


job.setJarByClass(map_reduce.class);
job.setMapperClass(MyMapper.class);
job.setReducerClass(MyReducer.class);

job.setNumReduceTasks(27);

job.setMapOutputKeyClass(CompositeKey.class);
job.setMapOutputValueClass(Text.class);

job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);

FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
