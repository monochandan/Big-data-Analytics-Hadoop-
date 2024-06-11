package ex.ex04;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class Task2bUserStatistics extends Configured implements Tool {

  public static class MyMap
       extends Mapper<Object, Text, Text, IntWritable>{

    private IntWritable length = new IntWritable();
    private Text userID = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
    {
    	String text=value.toString();
    	
    	Map<String, String> map=MRDPUtils.transformXmlToMap(text);

    	String userid=map.get("OwnerUserId");
    	String body=map.get("Body");

    	if ((userid!=null)&&(body!=null))
    	{
    		userID.set(userid);
    		length.set(body.length());
        	context.write(userID,length);
    	}
    }
  }

  public static class MyReduce
       extends Reducer<Text,IntWritable,Text,DoubleWritable> {
	    private DoubleWritable result = new DoubleWritable();
	    private Text resultKey = new Text();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	// note that the initial value for the minimum is MAX_VALUE
    	// to be sure that the real minimum is smaller than the initial value
    	double min=Double.MAX_VALUE,max=0,sum=0;

    	// a list for storing the values read from the Iterable
    	// since we'll later need to access the median element
    	// an ArrayList is more efficient for direct positional access,
    	// but insertion is expensive if the initial size is too small
    	List<Integer> list=new ArrayList<>(1000);
    	
    	for (IntWritable val : values)
    	{
    		int length=val.get();
    		list.add(length);
    		min=Math.min(min, length);
    		max=Math.max(max, length);
    		sum+=length;
    	}
    	if (list.size()>0)
    	{
    	resultKey.set(key.toString()+":min");
    	result.set(min);
        context.write(resultKey, result);

    	resultKey.set(key.toString()+":max");
    	result.set(max);
        context.write(resultKey, result);

        // median:
        // for lists with odd size, it is the element in the middle of the list
        // for lists with even size, it is the average of the two middle elements
        
        Collections.sort(list);
    	resultKey.set(key.toString()+":median");
    	//System.out.println(list.size()+"\t"+(int)(list.size()/2)+"\t"+((int)(list.size()/2)-1));
    	if (list.size()%2==1)
    		result.set(list.get((int)(list.size()/2)));
    	else
    		result.set(((double)(list.get((int)(list.size()/2)))+list.get((int)(list.size()/2)-1))/2);
        context.write(resultKey, result);

    	resultKey.set(key.toString()+":average");
    	result.set(sum/list.size());
        context.write(resultKey, result);
    	}
    }
  }
  @Override
  public int run(String[] args) throws Exception
  {
	    Configuration conf = getConf();
	    
	    Job job = Job.getInstance(conf, "userstats");
	    job.setJarByClass(Task2bUserStatistics.class);
	    job.setMapperClass(MyMap.class);
	    job.setReducerClass(MyReduce.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    // FileInputFormat will automatically decompress compressed input,
	    // but split it into blocks only if the compression is splittable
	    FileInputFormat.addInputPath(job, new Path("data/input/ex04-compressed"));
	    FileOutputFormat.setOutputPath(job, new Path("data/output/ex04/Task2b"));
	    
	    // example for deleting the output directory
	    Path output=new Path("data/output/ex04/Task2b");
	    output.getFileSystem(conf).delete(output,true);
	    
	    return job.waitForCompletion(true)?0:1;
  }
    public static void main(String[] args) throws Exception 
    {
    	ToolRunner.run(new Configuration(),new Task2bUserStatistics(), args);
    }
}