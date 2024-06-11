 
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

public class Assignment5Task1a extends Configured implements Tool {

  public static class MyMap
       extends Mapper<Object, Text, Text, Text>{
    
    private final static Text join = new Text();
    private final static Text txt = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
    {
    	String text=value.toString();
    	
    	Map<String, String> map=MRDPUtils.transformXmlToMap(text);
    	
    	String userid=map.get("OwnerUserId");

    	if (userid==null) userid=map.get("Id");
    	if (userid!=null)
    	{
    		join.set(userid);
    		txt.set(text);
        	context.write(join,txt);
    	}
    }
  }

  public static class MyReduce
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();


    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
        int uid = 0;
        String uname="";
        String views="";
        int count = 0;
        double score=0.0;
        String avgscore="";
        
        for (Text val : values)
        {
      	  String text=val.toString();
      	  Map<String, String> map=MRDPUtils.transformXmlToMap(text);
      	  
      	  //System.out.println(map);
      	  String id=map.get("Id");
      	  if (id!=null)
      	  {
      		  //System.out.println(id);
      		  uname=map.get("DisplayName");
      		  views=map.get("Views");
    		  uid=Integer.valueOf(map.get("Id"));
    		  
      	  }
      	  String s=map.get("Score");
      	  if (s!=null)
      	  { 
      		//System.out.println(s);
      		score+=Integer.parseInt(s);
      		count++;
      	  }
      	  avgscore = String.format("%.4g%n", (score/count));
      	  
        }
        
        if (uid >= 20 && uid <= 40)
		  {
			  result.set("Count: "+count+"; Name of User: "+uname+"; views:"+views+"");
			  context.write(key, result);
		  }
        
    }
  }
  @Override
  public int run(String[] args) throws Exception
  {
	    Configuration conf = getConf();
	    
	    Job job = Job.getInstance(conf, "usercounter");
	    job.setJarByClass(Assignment5Task1a.class);
	    job.setMapperClass(MyMap.class);
	    job.setReducerClass(MyReduce.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    return job.waitForCompletion(true)?0:1;
  }
    public static void main(String[] args) throws Exception 
    {
    	ToolRunner.run(new Configuration(),new Assignment5Task1a(), args);
    }
}