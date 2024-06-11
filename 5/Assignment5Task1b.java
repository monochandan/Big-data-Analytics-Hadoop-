
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.apache.hadoop.shaded.org.apache.commons.io.FileUtils;
import org.apache.hadoop.shaded.org.apache.curator.shaded.com.google.common.io.Files;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mrdp.utils.MRDPUtils;

public class Assignment5Task1b extends Configured implements Tool {

  public static class Mapsidejoin
       extends Mapper<Object, Text, Text, NullWritable>{

    private static Text ID = new Text();
    private static NullWritable rslt = NullWritable.get();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
    {
    	String text=value.toString();
    	
    	Map<String, String> map=MRDPUtils.transformXmlToMap(text);
    	
    	String userid=map.get("OwnerUserId");
    	if ((userid!=null)&&((Integer.valueOf(userid)>10)&&(Integer.valueOf(userid)<15)))
    	{
    		String d=map.get("Id");
    		ID.set("Userid: "+userid+" ;Id:"+d+"");
        	context.write(ID,rslt);
    	}
    }
  }

  Collection<Object> set = Stream.of()
		  .collect(Collectors.toCollection(HashSet::new));
  
  public Assignment5Task1b(Object file) throws IOException
  {
	
	String path="C:/Users/jibin/Downloads/user10000.xml";
	List<String> lines= FileUtils.readLines(new File(path));

	for (String line: lines)
	   {
	     Map<String, String> map=MRDPUtils.transformXmlToMap(line);

	     String id=map.get("Id");
	     if (id!=null) 
	     {
	    	set.add(id);
	     }
	    }
  }
  
  @Override
  public int run(String[] args) throws Exception
  {
	    Configuration conf = getConf();
	    
	    Job job = Job.getInstance(conf, "map-side join");
	    job.setJarByClass(Assignment5Task1b.class);
	    job.setMapperClass(Mapsidejoin.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    job.setNumReduceTasks(0);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    return job.waitForCompletion(true)?0:1;
  }
    public static void main(String[] args) throws Exception 
    {
    	ToolRunner.run(new Configuration(),new Assignment5Task1b(args), args);
    }
}