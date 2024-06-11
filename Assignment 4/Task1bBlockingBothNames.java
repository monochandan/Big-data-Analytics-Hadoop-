package ex.ex04;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mrdp.utils.MRDPUtils;

public class Task1bBlockingBothNames extends Configured implements Tool {

  public static class MyMap
       extends Mapper<Object, Text, Text, Text>{

	    private Text name = new Text();
	    private Text blockID = new Text();

    private static String[] splitLine(String line)
    {
    	int idx=line.indexOf(',');
    	if (idx==-1) return new String[0];
    	else
    	{
    		String[] arr=new String[2];
    		arr[0]=line.substring(0, idx);
    		if (idx<line.length())
    			arr[1]=line.substring(idx+1);
    		return arr;
    	}
    }
    
    private static String getFirstName(String name)
    {
    	name=name.trim();
    	int idx=name.indexOf(' ');
    	if (idx==-1) return name;
    	else
    	{
    		return name.substring(0, idx);
    	}
    }
    
    private static String getLastName(String name)
    {
    	name=name.trim();
    	int idx=name.lastIndexOf(' ');
    	if (idx==-1) return name;
    	else
    	{
    		return name.substring(idx+1);
    	}
    }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
    {
    	String text=value.toString();
    	
    	String names[]=splitLine(text);
    	if (names.length!=2) return;

    	for (String n: names)
    	{
    		if (n.length()<1) continue;
    		name.set(n);
    		blockID.set(getFirstName(n).substring(0, 1)+getLastName(n));
        	context.write(blockID,name);
    	}
    }
  }

  public static class MyReduce
       extends Reducer<Text,Text,Text,NullWritable> {
	    private NullWritable result = NullWritable.get();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      Set<String> names=new HashSet<>();
      
      if (key.toString().compareTo("DSchmidt")==0)
      {
	      for (Text val : values)
	      {
			  context.write(val,result);
	      }

      }
    }
  }
  @Override
  public int run(String[] args) throws Exception
  {
	    Configuration conf = getConf();
	    
	    Job job = Job.getInstance(conf, "MixedBlocker");
	    job.setJarByClass(Task1bBlockingBothNames.class);
	    job.setMapperClass(MyMap.class);
	    job.setReducerClass(MyReduce.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    
	    Path output=new Path("data/output/ex04/Task1b");
	    output.getFileSystem(conf).delete(output,true);

	    FileInputFormat.addInputPath(job, new Path("data/input/ex04/dblp_names.csv"));
	    FileOutputFormat.setOutputPath(job, new Path("data/output/ex04/Task1b"));
	    return job.waitForCompletion(true)?0:1;
  }
    public static void main(String[] args) throws Exception 
    {
    	ToolRunner.run(new Configuration(),new Task1bBlockingBothNames(), args);
    }
}