package ex.ex05;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.hadoop.shaded.org.apache.curator.shaded.com.google.common.io.Files;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mrdp.utils.MRDPUtils;

public class Task1bMapSideJoin extends Configured implements Tool {

	final static String userfile="data/input/ex05/task1/user10000.xml";
	
  public static class MyMap
       extends Mapper<Object, Text, Text, NullWritable>{

    private final static Text resultID = new Text();
    private final static NullWritable result = NullWritable.get();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
    {
    	String text=value.toString();
    	
    	Map<String, String> map=MRDPUtils.transformXmlToMap(text);

//    	System.out.println(text);
    	String userid=map.get("OwnerUserId");
//    	System.out.println(userid);
    	if ((userid!=null)&&(idset.contains(userid))&&(Integer.valueOf(userid)>=10)&&(Integer.valueOf(userid)<=15))
    	{
//    		resultID.set(map.get("OwnerUserId"));
    		resultID.set(map.get("Id"));
//    		resultID.set(userid+" "+map.get("Id")); // alternative: userid + postid
        	context.write(resultID,result);
    	}
    }
  }

  static Set<String> idset=new HashSet<>();
  
  public Task1bMapSideJoin()
  {
	List<String> lines=null;
	
	try
	{
		lines=Files.readLines(new File(userfile), Charset.forName("UTF-8"));
	}
	catch(Exception e)
	{
		System.err.println(e);
	}
	
	for (String line: lines)
	{
    	Map<String, String> map=MRDPUtils.transformXmlToMap(line);

    	String id=map.get("Id");
    	if (id!=null) idset.add(id);
	}
	
  }
  
  @Override
  public int run(String[] args) throws Exception
  {
	    Configuration conf = getConf();
	    
	    Job job = Job.getInstance(conf, "map-side join");
	    job.setJarByClass(Task1bMapSideJoin.class);
	    job.setMapperClass(MyMap.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    job.setNumReduceTasks(0);
	    
	    Path output=new Path("data/output/ex05/task1b");
	    output.getFileSystem(conf).delete(output,true);

	    FileInputFormat.addInputPath(job, new Path("data/input/ex05/task1b"));
	    FileOutputFormat.setOutputPath(job, output);
	    return job.waitForCompletion(true)?0:1;
  }
    public static void main(String[] args) throws Exception 
    {
    	ToolRunner.run(new Configuration(),new Task1bMapSideJoin(), args);
    }
}