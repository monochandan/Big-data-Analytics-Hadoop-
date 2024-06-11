package ex.ex06;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

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

public class Task2MinHashingLSH extends Configured implements Tool{

	final static int shingleSize=5;
	final static int numHashFunctions=10;
	
	final static int subsequenceLength=3;
	
	// approximate upper bound of number of shingles
	final static int NUM_SHINGLES=90000;
	
	// Mapper:
	// * for each input line, generate the corresponding k-shingles and
	//   the MinHash signature with m hash functions
	// * Given a MinHash signature with m entries, the mapper should emit subsequences of
	//   length p as key and the docid as the value.
	
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
	    	
	    	// consider all possible start indexes for a subsequence to emit
	    	for (int p=0;p<signature.length-subsequenceLength;p++)
	    	{
	    		StringBuilder blockid=new StringBuilder();
	    		for (int i=0;i<subsequenceLength;i++)
	    		{
	    			if (blockid.length()>0) blockid.append(",");
	    			blockid.append(signature[p+i]);
	    		}
	    		block.set((p+1)+":"+blockid.toString());
	    		docID.set(docid);
//	    		System.out.println(block.toString()+" "+docID.toString());
		       	context.write(block,docID);
	    	}
    	}
	}

	// Reducer:
	// * The reducer checks if it received more than one value for a key. 
	// * In this case, the reducer 	should output the key and the complete
	//   list of values received if the list of values includes docid 375.
	
 public static class MyReduce
      extends Reducer<Text,Text,Text,Text> {
	    private Text result = new Text();

   public void reduce(Text key, Iterable<Text> values,
                      Context context
                      ) throws IOException, InterruptedException {

//	   System.out.println(key.toString());

	   List<String> list=new LinkedList<>();
	   
	   for (Text v: values)
		   list.add(v.toString());
	   
//	   if (list.size()>1)
	   {
		   if (list.contains("1282"))
		   {
			   result.set(list.toString());
			   context.write(key, result);
		   }
	   }
   }
 }

	  @Override
	  public int run(String[] args) throws Exception
	  {
		    Configuration conf = getConf();
		    
		    Job job = Job.getInstance(conf, "MinHashing/LSH");
		    job.setJarByClass(Task2MinHashingLSH.class);
		    job.setMapperClass(MyMap.class);
		    job.setReducerClass(MyReduce.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
		    Path output=new Path("data/output/ex06/Task6-"+subsequenceLength);
		    output.getFileSystem(conf).delete(output,true);

		    FileInputFormat.addInputPath(job, new Path("data/input/ex03/corpus_with_line_numbers.txt"));
		    FileOutputFormat.setOutputPath(job, new Path("data/output/ex06/Task6-"+subsequenceLength));
		    return job.waitForCompletion(true)?0:1;
	  }
	    public static void main(String[] args) throws Exception 
	    {
	    	ToolRunner.run(new Configuration(),new Task2MinHashingLSH(), args);
	    }
}
