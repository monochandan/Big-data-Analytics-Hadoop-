import java.io.IOException;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class task1a extends Configured implements Tool{
	
	public static class Map extends Mapper<Object, Text, Text, Text>
	{
		private Text name = new Text();
		private Text block = new Text();
		
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		String input = value.toString();
		String[] names = input.split(",");
		// Set<String>s1 = new HashSet<>(); // for the block s1
		for(String element: names)
		{
			name.set(element);
			String lastname = element.substring(element.lastIndexOf(" ")+1);
			block.set(lastname);
			context.write(block, name);
			
		}
	
	}
		
	}
	
	public static class reducer extends Reducer<Text, Text, Text, Text>
	{
		private Text result = new Text();
		Set names = new HashSet();
		String temp;
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException
		{
			if(key.toString().compareTo("Miller") == 0)
			{
				for(Text n : value)
				{
					temp = n.toString();
					names.add(temp+"\n");
					
				}
				result.set(names.toString()); 
				context.write(key, result);
			}
		}
		
	}
	
	@Override
	  public int run(String[] args) throws Exception
	  {
Configuration conf = getConf();
	    
	    Job job = Job.getInstance(conf, "task1a");
	    job.setJarByClass(task1a.class);
	    job.setMapperClass(Map.class);
	    job.setReducerClass(reducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    return job.waitForCompletion(true)?0:1;
	  } 
	    public static void main(String[] args) throws Exception 
	    {
	    	ToolRunner.run(new Configuration(), new task1a(), args);
	    }

}
