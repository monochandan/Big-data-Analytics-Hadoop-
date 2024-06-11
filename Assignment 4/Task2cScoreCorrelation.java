package ex.ex04;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class Task2cScoreCorrelation extends Configured implements Tool {

  public static class MyMap
       extends Mapper<Object, Text, Text, Text>{

	// encodes a pair of body length and score, separated by $
	private Text pair=new Text();
	
	// this is the output key for the mapper
	// it is chosen arbitrarily, but must be the same for all map outputs
    private final Text word = new Text("1");

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
    {
    	String text=value.toString();
    	
    	Map<String, String> map=MRDPUtils.transformXmlToMap(text);

    	String title=map.get("Title");
    	String score=map.get("Score");

    	if ((title!=null)&&(score!=null))
    	{
    		// 101$40
    		pair.set(title.length()+"$"+score); // simple solution ;-)
        	context.write(word,pair);
    	}
    }
  }

  public static class MyReduce
       extends Reducer<Text,Text,NullWritable,DoubleWritable> {
	    private DoubleWritable result = new DoubleWritable();
	    
	    // no need for grouping the output values, so we use a NullWritable as key
	    private NullWritable resultKey = NullWritable.get();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

    	// we are buffering the extracted values in in-memory lists
    	// to avoid multiple extractions from Strings
    	
    	List<Double> scores=new LinkedList<>(),lengths=new LinkedList<>();
    	
    	// implementation of sample correlation coefficients based on
    	// https://en.wikipedia.org/wiki/Pearson_correlation_coefficient#For_a_sample
    	
    	// note that there may be other, slightly different formulae for computing this
    	
    	double alength=0,ascore=0;
    	
    	for (Text v: values)
    	{
    		// extract length and score from the pair and store them in the corresponding lists
    		String pair=v.toString();
    		int idx=pair.indexOf('$');
    		if (idx==-1) continue;
    		double length=Double.valueOf(pair.substring(0, idx));
    		lengths.add(length); alength+=length;
    		double score=Double.valueOf(pair.substring(idx+1));
    		scores.add(score); ascore+=score;
    	}
    	alength=alength/lengths.size(); // average length
    	ascore=ascore/scores.size(); // average score
    	
    	Iterator<Double> lit=lengths.iterator();
    	Iterator<Double> sit=scores.iterator();

    	// compute the covariance of length and score,
    	// the variance of length, and the variance of score
    	// note that we do not divide by the number of samples since this cancels out later
    	
    	double cov=0;
    	double lvar=0,svar=0;
 
    	// iterate over the samples
    	for (int i=0;i<scores.size();i++)
    	{
    		double l=lit.next();
    		double s=sit.next();
    		
    		cov+=(l-alength)*(s-ascore);
    		lvar+=(l-alength)*(l-alength);
    		svar+=(l-ascore)*(l-ascore);
    	}
    	
    	// compute correlation coefficient based on the formula above
    	double corr=cov/(Math.sqrt(lvar)*Math.sqrt(svar));
    	
    	result.set(corr);
        context.write(resultKey, result);
    }
  }
  @Override
  public int run(String[] args) throws Exception
  {
	    Configuration conf = getConf();
	    
	    Job job = Job.getInstance(conf, "correlation");
	    job.setJarByClass(Task2cScoreCorrelation.class);
	    job.setMapperClass(MyMap.class);
	    job.setReducerClass(MyReduce.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    Path output=new Path("data/output/ex04/Task2c");
	    output.getFileSystem(conf).delete(output,true);

	    FileInputFormat.addInputPath(job, new Path("data/input/ex04-compressed"));
	    FileOutputFormat.setOutputPath(job, new Path("data/output/ex04/Task2c"));
	    return job.waitForCompletion(true)?0:1;
  }
    public static void main(String[] args) throws Exception 
    {
    	ToolRunner.run(new Configuration(),new Task2cScoreCorrelation(), args);
    }
}