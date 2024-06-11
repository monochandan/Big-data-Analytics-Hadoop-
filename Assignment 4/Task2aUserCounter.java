package ex.ex04;
import java.io.IOException;
import java.util.Map;
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

import mrdp.utils.MRDPUtils;

public class Task2aUserCounter extends Configured implements Tool {

  public static class MyMap
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text userID = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
    {
    	String text=value.toString();
    	
    	Map<String, String> map=MRDPUtils.transformXmlToMap(text);

    	String userid=map.get("OwnerUserId");
    	if (userid!=null)
    	{
    		userID.set(userid);
        	context.write(userID,one);
    	}
    }
  }

  public static class MyReduce
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int cnt = 0;
      for (IntWritable val : values) cnt+=val.get();
      result.set(cnt);
      context.write(key, result);
    }
  }
  @Override
  public int run(String[] args) throws Exception
  {
	    Configuration conf = getConf();
	    
	    Job job = Job.getInstance(conf, "usercounter");
	    job.setJarByClass(Task2aUserCounter.class);
	    job.setMapperClass(MyMap.class);
	    job.setReducerClass(MyReduce.class);
	    job.setCombinerClass(MyReduce.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    Path output=new Path("data/output/ex04/Task2a");
	    output.getFileSystem(conf).delete(output,true);

	    FileInputFormat.addInputPath(job, new Path("data/input/ex04-compressed"));
	    FileOutputFormat.setOutputPath(job, new Path("data/output/ex04/Task2a"));
	    return job.waitForCompletion(true)?0:1;
  }
    public static void main(String[] args) throws Exception 
    {
    	ToolRunner.run(new Configuration(),new Task2aUserCounter(), args);
    }
}