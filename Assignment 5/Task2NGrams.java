package ex.ex05;
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

public class Task2NGrams extends Configured implements Tool {

    final static int sigma_min=2; // min length of n-gram
    final static int sigma_max=4; // max length of n-gram
    final static int tau=1000;   // lower threshold for emitting an n-gram
    

  public static class MyMap
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text ngram = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
    {
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

    	StringTokenizer tokenizer=new StringTokenizer(input);
    	String[] tokens=new String[tokenizer.countTokens()];
    	for (int pos=0;pos<tokens.length;pos++)
    		tokens[pos]=tokenizer.nextToken();
    	
    	for (int pos=0;pos<tokens.length;pos++)
    	{
    		String gram="";
    		for (int delta=0;delta<sigma_max;delta++)
    		{
    			if (pos+delta==tokens.length) break;
    			if (delta==0) gram=tokens[pos+delta];
    			else gram=gram+" "+tokens[pos+delta];
    			
    			if (delta<sigma_min-1) continue;
    			ngram.set(gram);
    			context.write(ngram, one);
    		}
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
      if (cnt<tau) return; 
      result.set(cnt);
      context.write(key, result);
    }
  }
  @Override
  public int run(String[] args) throws Exception
  {
	    Configuration conf = getConf();
	    
	    Job job = Job.getInstance(conf, "simple n-grams");
	    job.setJarByClass(Task2NGrams.class);
	    job.setMapperClass(MyMap.class);
	    job.setReducerClass(MyReduce.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    Path output=new Path("data/output/ex05/ngram");
	    output.getFileSystem(conf).delete(output,true);

	    FileInputFormat.addInputPath(job, new Path("data/input/ex05/corpus.txt"));
	    FileOutputFormat.setOutputPath(job, output);
	    return job.waitForCompletion(true)?0:1;
  }
    public static void main(String[] args) throws Exception 
    {
    	ToolRunner.run(new Configuration(),new Task2NGrams(), args);
    }
}