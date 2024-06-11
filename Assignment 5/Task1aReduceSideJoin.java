package ex.ex05;
import java.io.IOException;
import java.util.Map;

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

public class Task1aReduceSideJoin extends Configured implements Tool {

  public static class MyMap
       extends Mapper<Object, Text, Text, Text>{

    private final static Text joinAttribute = new Text();
    private final static Text line = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
    {
    	String text=value.toString();
    	
    	Map<String, String> map=MRDPUtils.transformXmlToMap(text);

//    	System.out.println(text);
    	String userid=map.get("OwnerUserId"); // it's a post
//    	System.out.println(userid);
    	if (userid==null) userid=map.get("Id"); // it's a user
    	if (userid!=null)
    	{
    		joinAttribute.set(userid);
    		line.set(text);
        	context.write(joinAttribute,line);
    	}
    }
  }

  public static class MyReduce
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int cnt = 0;
      double score=0.0;
      String userid=null;
      String username=null;
      String views=null;
      
      for (Text val : values)
      {
    	  String text=val.toString();
    	  Map<String, String> map=MRDPUtils.transformXmlToMap(text);
    	  
    	  String id=map.get("OwnerUserId");
    	  if (id!=null)
    	  {
    		  // this is a line from post10000.xml
        	  String scoreS=map.get("Score");
        	  if (scoreS!=null)
        	  {
        		  score+=Double.parseDouble(scoreS);
        		  cnt++;
        	  }
    	  }
    	  else
    	  {
    		  username=map.get("DisplayName");
    		  userid=map.get("Id");
    		  views=map.get("Views");
    	  }
      }
      
      if ((userid!=null)&&(cnt>0)&&((Integer.valueOf(userid)>=20)&&(Integer.valueOf(userid)<41)))
      {
          result.set("Count: "+cnt+"; avg score: "+score/cnt+"; name: "+username+"; views:"+views);
          context.write(key, result);    	  
      }
    }
  }
  @Override
  public int run(String[] args) throws Exception
  {
	    Configuration conf = getConf();
	    
	    Job job = Job.getInstance(conf, "reduce-side join");
	    job.setJarByClass(Task1aReduceSideJoin.class);
	    job.setMapperClass(MyMap.class);
	    job.setReducerClass(MyReduce.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    Path output=new Path("data/output/ex05/task1a");
	    output.getFileSystem(conf).delete(output,true);

	    FileInputFormat.addInputPath(job, new Path("data/input/ex05/task1a"));
	    FileOutputFormat.setOutputPath(job, output);
	    return job.waitForCompletion(true)?0:1;
  }
    public static void main(String[] args) throws Exception 
    {
    	ToolRunner.run(new Configuration(),new Task1aReduceSideJoin(), args);
    }
}