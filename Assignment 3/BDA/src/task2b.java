import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import java.util.Map;
import mrdp.utils.*;

public class task2b extends Configured implements Tool {
	
		public static class Map extends Mapper<Object, Text, Text, IntWritable>
		{
			private final static IntWritable length = new IntWritable(1);
			private Text id = new Text();
			
			public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String input=value.toString();
    	
    	HashMap<String, String> mapping = new HashMap<String, String>();
    	mapping = (HashMap<String, String>) MRDPUtils.transformXmlToMap(input);
        
        String userId = mapping.get("OwnerUserId");
        String title = mapping.get("Title");
        
      	  if(userId != null && userId.length() <= 2)
      	  {
      		  if(title != null)
      		  {
      			id.set(userId);
      			length.set(title.length());
        		context.write(id, length);
      		  }
      		  
      	  }
        
		}
	}
		
		public static class reducer
	       extends Reducer<Text,IntWritable,Text,DoubleWritable> {
	    private DoubleWritable result = new DoubleWritable();

	    public void reduce(Text key, Iterable<IntWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException, IndexOutOfBoundsException {
	      int sum = 0;
	      double minimal = 0.0, maximal = 0.0, median = 0.0, average=0.0;
	      int length_of_title = 0;
	      int sum_of_all_title_length = 0;
	      int index = 0,val1 = 0, val2 = 0;
	      int size = 0;
	      List<Double>titles_for_the_id = new ArrayList<>();
	      /// every time for one key
	      for (IntWritable val : values)
	      {
	    	  sum++;
	    	  length_of_title = val.get();// length of the title
	    	  sum_of_all_title_length += length_of_title; // summing the all titles for that id 
	    	  titles_for_the_id.add((double) length_of_title); // adding the every title length in the list
	    	  minimal = Collections.min(titles_for_the_id); // calculating the minimum
	    	  maximal = Collections.max(titles_for_the_id); // calculating the maximum
	    	  size = titles_for_the_id.size(); // calculating the total size of the list for the id
	    	  //average += sum_of_all_title_length; // calculating the average
	    	  
	    	  // we need to sort the collection for calculate the median value
	    	  Collections.sort(titles_for_the_id); // sorting the list
	    	  /*if(size % 2 != 0) // checking if the size is odd
	    	  {
	    		  index = (size+1)/2;
	    		  median = titles_for_the_id.get(index);
	    	  }
	    		  
	    	else if(size %2 == 0)// for even size
	    	{
	    		index = (size/2);
	    		val1 = titles_for_the_id.get(index);
	    		index = (size/2) + 1;
	    		val1 = titles_for_the_id.get(index);
	    		median = (val1 + val2)/2;
	    		
	    	}
	    	else
	    	{
	    		index = size;
	    		median = titles_for_the_id.get(index);
	    	}*/
	    	  
	    	double middle ;
	    	middle = titles_for_the_id.size() / 2.0;
			middle = middle > 0 && middle % 2 == 0 ? middle - 1 : middle;			// calculating median from the listOfValues
			median = titles_for_the_id.get((int) middle);
	    		
	   
	      }
	      
	      Text txt = new Text();
	      String str = " ";
	      if(sum > 0)
	      {
	    	  str = key.toString();
	    	  txt.set(str+":minimal");
	    	  result.set((double)minimal);
	    	  context.write(txt, result);
	    	  
	    	  str = key.toString();
	    	  txt.set(str+":maximal");
	    	  result.set((double)maximal);
	    	  context.write(txt, result);
	    	  
	    	  str = key.toString();
	    	  txt.set(str+":median");
	    	  result.set((double)median);
	    	  context.write(txt, result);
	    	  
	    	  str = key.toString();
	    	  txt.set(str+":average");
	    	  result.set((double)sum_of_all_title_length / size);
	    	  context.write(txt, result);
	    	  
	    	  
	      }
	       
	      
	      //result.set(sum);
	      //context.write(key, result);
	    }
	  }
	  @Override
	  public int run(String[] args) throws Exception
	  {
		    Configuration conf = getConf();
		    
		    Job job = Job.getInstance(conf, "task2b");
		    job.setJarByClass(task2b.class);
		    job.setMapperClass(Map.class);
		    job.setReducerClass(reducer.class);
		    job.setOutputKeyClass(Text.class);
		    //job.setOutputValueClass(IntWritable.class);
		    job.setOutputValueClass(DoubleWritable.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    return job.waitForCompletion(true)?0:1;
	  }
	    public static void main(String[] args) throws Exception 
	    {
	    	ToolRunner.run(new Configuration(),new task2b(), args);
	    }
}

