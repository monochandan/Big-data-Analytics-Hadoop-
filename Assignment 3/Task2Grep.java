package ex.ex03;
import java.io.IOException;
import java.util.HashSet;
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

public class Task2Grep extends Configured implements Tool {

  public static class Map
       extends Mapper<Object, Text, Text, NullWritable>{

	  	private static IntWritable one = new IntWritable(1);
	    private Text docidText = new Text();

    private static String wordToFind="MapReduce";
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String line=value.toString();
    	
    	int idx=line.indexOf(':');
    	if ((idx==-1)||(idx==line.length())) return;
    	String docid=line.substring(0, idx);
    	String text=line.substring(idx+1);
    	
    	if (text.contains(wordToFind))
    	{
    		docidText.set(docid);
    		context.write(docidText,NullWritable.get());
    	}
    }
  }

  @Override
  public int run(String[] args) throws Exception
  {
	    Configuration conf = getConf();
	    
	    Job job = Job.getInstance(conf, "grep");
	    job.setJarByClass(Task2Grep.class);
	    job.setMapperClass(Map.class);
	    job.setNumReduceTasks(0);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);

	    Path output=new Path(args[1]);
	    output.getFileSystem(conf).delete(output,true);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, output);
	    
	    return job.waitForCompletion(true)?0:1;
  } 
    public static void main(String[] args) throws Exception 
    {
    	ToolRunner.run(new Configuration(),new Task2Grep(), args);
    }
}