package ex.ex06;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Task2MinHashingLSHFull extends Configured implements Tool{

	final static int shingleSize=5;
	final static int numHashFunctions=100;
	
	final static int subsequenceLength=3;
	
	// approximate upper bound of number of shingles
	final static int NUM_SHINGLES=90000;

	final static double THRESHOLD=0.5;
	
	public enum COUNTERS
	{
		COMPARISONS, MATCHES
	}
	
	static String pack(int[] array)
	{
		StringBuilder sb=new StringBuilder();
		for (int i: array)
		{
			if (sb.length()>0) sb.append("&");
			sb.append(i);
		}
		return sb.toString();
	}
	
	static int[] unpack(String s)
	{
		String parts[]=s.split("&");
		int result[]=new int[parts.length];
		for (int i=0;i<parts.length;i++)
			result[i]=Integer.valueOf(parts[i]);
		return result;
	}
	
	static double overlap(int[] sig1, int[] sig2)
	{
		if (sig1.length!=sig2.length) return 0.0;
		
		int common=0;
		for (int i=0;i<sig1.length;i++)
			if (sig1[i]==sig2[i]) common=common+1;
		return ((double)common)/sig1.length;
	}
	
	// Mapper
	// main addition: emit not only the key, but also the document's signature in an encoded form
	// the output of the mapper looks similar to this:
	// key: 1:2,6 (as before)
	// value: 1234$2,6,5,12,6 (with the docid before the $ and the signature after, components separated by &)
	
	public static class MyMap extends Mapper<Object, Text, Text, Text>
	{
	    private Text block = new Text();
	    private Text docID = new Text();
	
	    Task1cMinHashing minhasher=new Task1cMinHashing(numHashFunctions,shingleSize);
	    
	    public void map(Object key, Text value, Context context)
	    		    throws IOException, InterruptedException 
	    {
		   	String line=value.toString();
		   	
	    	int idx=line.indexOf(':');
	    	if ((idx==-1)||(idx==line.length())) return;
	    	String docid=line.substring(0, idx).trim();
	    	String text=line.substring(idx+1).trim();

	    	int[] signature=minhasher.computeSignature(text,NUM_SHINGLES);

//	    	System.out.println(docid+" "+Arrays.toString(signature));
	    	
	    	for (int p=0;p<signature.length-subsequenceLength+1;p++)
	    	{
	    		StringBuilder blockid=new StringBuilder();
	    		for (int i=0;i<subsequenceLength;i++)
	    		{
	    			if (blockid.length()>0) blockid.append(",");
	    			blockid.append(signature[p+i]);
	    		}
	    		block.set((p+1)+":"+blockid.toString());
	    		docID.set(docid+"$"+pack(signature));
//	    		System.out.println(block.toString()+" "+docID.toString());
		       	context.write(block,docID);
	    	}
    	}
	}

	// Reducer:
	// if a key has more than one docid assigned,
	// extract the signatures from the values and compute their similarities
	// emit those pairs where the similarity is beyond the threshold
	
 public static class MyReduce
      extends Reducer<Text,Text,Text,Text> {
	    private Text pair = new Text();
	    private Text result = new Text();

   public void reduce(Text key, Iterable<Text> values,
                      Context context
                      ) throws IOException, InterruptedException {

//	   System.out.println(key.toString());

	   List<String> list=new LinkedList<>();
	   
	   for (Text v: values)
		   list.add(v.toString());
	   
	   if (list.size()>1)
	   {
		   Map<Integer,int[]> map=new HashMap<>();
		   
		   for (String s: list)
		   {
			   // format: docid$d1&d2...
			   
			   int idx=s.indexOf('$');
			   Integer docid=Integer.valueOf(s.substring(0, idx));
			   int signature[]=unpack(s.substring(idx+1));
			   map.put(docid, signature);
		   }
		   
		   if (map.keySet().contains(1282)==false) return;

		   int cnt=0;
			int outer=0;
			int matches=0;
			for (Integer d1: map.keySet())
			{
				outer++;
				if (outer%100==0) System.out.println(outer);
				
				int[] s1=map.get(d1);
				for (Integer d2: map.keySet())
				{
					if (d1>=d2) continue;

					cnt++;

					double sim=overlap(s1,map.get(d2));
//					System.out.println("sim("+d1+","+d2+")="+sim);

					if (sim>=THRESHOLD) 
					{
						matches++;
//						System.out.println("sim("+d1+","+d2+")="+sim);
						pair.set(d1+","+d2);
						result.set(sim+"");
					    context.write(pair, result);
					}
				}
			}
//			System.out.println("overall "+cnt+" comparisons, "+matches+" matches.");
			
			context.getCounter(COUNTERS.COMPARISONS).increment(cnt);
			context.getCounter(COUNTERS.MATCHES).increment(matches);
	   }
   }
 }

	  @Override
	  public int run(String[] args) throws Exception
	  {
		    Configuration conf = getConf();
		    
		    Job job = Job.getInstance(conf, "MinHashing/LSH");
		    job.setJarByClass(Task2MinHashingLSHFull.class);
		    job.setMapperClass(MyMap.class);
		    job.setReducerClass(MyReduce.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
		    Path output=new Path("data/output/ex06/Task6b-"+subsequenceLength);
		    output.getFileSystem(conf).delete(output,true);

		    FileInputFormat.addInputPath(job, new Path("data/input/ex03/corpus_with_line_numbers.txt"));
		    FileOutputFormat.setOutputPath(job, new Path("data/output/ex06/Task6b-"+subsequenceLength));
		    return job.waitForCompletion(true)?0:1;
	  }
	    public static void main(String[] args) throws Exception 
	    {
	    	ToolRunner.run(new Configuration(),new Task2MinHashingLSHFull(), args);
	    }
}
