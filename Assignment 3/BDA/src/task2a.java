import java.io.IOException;



import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.HashMap;
import mrdp.utils.*;

public class task2a extends Configured implements Tool {
	
		public static class Map extends Mapper<Object, Text, Text, IntWritable>
		{
			private final static IntWritable one = new IntWritable(1);
			private Text id = new Text();
			
			public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String input=value.toString();
    	
    	HashMap<String, String> mapping = new HashMap<String, String>();
    	mapping = (HashMap<String, String>) MRDPUtils.transformXmlToMap(input);
        
        String userId = mapping.get("OwnerUserId");
        
      	  if(userId != null && userId.length() <= 2)
      	  {
      		  id.set(userId);
      		  context.write(id, one);
      	  }
        
		}
	}
		
		public static class reducer
	       extends Reducer<Text,IntWritable,Text,IntWritable> {
	    private IntWritable result = new IntWritable();

	    public void reduce(Text key, Iterable<IntWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int count = 0;
	      for (IntWritable val : values)
	      {
	    	  
	    	  count += 1;
	      }
	       
	      
	      result.set(count);
	      context.write(key, result);
	    }
	  }
	  @Override
	  public int run(String[] args) throws Exception
	  {
		    Configuration conf = getConf();
		    
		    Job job = Job.getInstance(conf, "Task2a");
		    job.setJarByClass(task2a.class);
		    job.setMapperClass(Map.class);
		    job.setReducerClass(reducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    return job.waitForCompletion(true)?0:1;
	  }
	    public static void main(String[] args) throws Exception 
	    {
	    	ToolRunner.run(new Configuration(),new task2a(), args);
	    }
}
