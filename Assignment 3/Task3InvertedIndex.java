package ex.ex03;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeSet;

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

public class Task3InvertedIndex extends Configured implements Tool {

  public static class Map
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable docidWritable = new IntWritable(1);
    private final static Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	String line=value.toString()
    					  .replace('\"', ' ')
    					  .replace('!', ' ')
    					  .replace('\'', ' ')
    					  .replace('?', ' ')
    					  .replace('(', ' ')
    					  .replace(')', ' ')
    					  .replace('-', ' ')
    					  .replace('.', ' ')
    					  .replace(';', ' ')
    					  .replace('=', ' ')
    					  .replace(',',' ')
    					  .replace('[',' ')
    					  .replace(']',' ')
    					  .toLowerCase();
    	
    	int idx=line.indexOf(':');
    	if ((idx==-1)||(idx==line.length())) return;
    	String docid=line.substring(0, idx);
    	String text=line.substring(idx+1);

    	docidWritable.set(Integer.valueOf(docid));
    	
        StringTokenizer itr = new StringTokenizer(text);
        while (itr.hasMoreTokens()) {
          word.set(itr.nextToken());
          context.write(word, docidWritable);
      }
    }
  }

  public static class Reduce
       extends Reducer<Text,IntWritable,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
    TreeSet<Integer> docids=new TreeSet<>();
    for (IntWritable val : values) {
    	docids.add(val.get());
    }
    if (docids.size()<=3) return;
    
	  StringBuilder sb=new StringBuilder();
	  for (int val : docids) {
	    if (sb.length()>0) sb.append(", ");
	    sb.append(val);
	  }
      result.set(sb.toString());
      context.write(key, result);
    }
  }
  @Override
  public int run(String[] args) throws Exception
  {
	    Configuration conf = getConf();
	    
	    Job job = Job.getInstance(conf, "inverted index");
	    job.setJarByClass(Task3InvertedIndex.class);
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    Path output=new Path(args[1]);
	    output.getFileSystem(conf).delete(output,true);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, output);
	    
	    return job.waitForCompletion(true)?0:1;
  }
    public static void main(String[] args) throws Exception 
    {
    	ToolRunner.run(new Configuration(),new Task3InvertedIndex(), args);
    }
}