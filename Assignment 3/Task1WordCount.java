package ex.ex03;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
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

public class Task1WordCount extends Configured implements Tool {

  public static class Map
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

    	String input=value.toString()
    					  .replace('\"', ' ')
    					  .replace('!', ' ')
    					  .replace('\'', ' ')
    					  .replace('?', ' ')
    					  .replace('(', ' ')
    					  .replace(')', ' ')
    					  .replace('-', ' ')
    					  .replace('.', ' ')
    					  .replace(':', ' ')
    					  .replace(';', ' ')
    					  .replace('=', ' ')
    					  .replace(',',' ')
    					  .replace('[',' ')
    					  .replace(']',' ')
    					  .toLowerCase();
    	
    	Set<String> tokens=new HashSet<String>();

	    StringTokenizer itr = new StringTokenizer(input);
	    while (itr.hasMoreTokens()) 
	    {
	    	tokens.add(itr.nextToken());
	    }
	    for (String token: tokens)
	    {
	    	word.set(token);
	    	context.write(word, one);
	    }
    }
  }

  public static class Reduce
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  @Override
  public int run(String[] args) throws Exception
  {
	    Configuration conf = getConf();
	    
	    Job job = Job.getInstance(conf, "word count++");
	    job.setJarByClass(Task1WordCount.class);
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    Path output=new Path(args[1]);
	    output.getFileSystem(conf).delete(output,true);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, output);
	    return job.waitForCompletion(true)?0:1;
  } 
    public static void main(String[] args) throws Exception 
    {
    	ToolRunner.run(new Configuration(),new Task1WordCount(), args);
    }
}