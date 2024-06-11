import java.io.IOException;



import java.util.*;
import java.util.Map.Entry;

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
import org.apache.hadoop.shaded.org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.shaded.org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import java.util.Map;
import mrdp.utils.*;

public class task2c extends Configured implements Tool {
	
		public static class Map extends Mapper<Object, Text, Text, Text>
		{
			//final Pair<String, String> pair = new MutablePair<>();
			//private final static IntWritable scoreVews = new IntWritable();
			private final Text one = new Text("1");
			
			private Text id = new Text();
			
			public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String input=value.toString();
    	
    	HashMap<String, String> mapping = new HashMap<String, String>();
    	mapping = (HashMap<String, String>) MRDPUtils.transformXmlToMap(input);
        
        //String userId = mapping.get("OwnerUserId");
        String score = mapping.get("Score");
        String vews =  mapping.get("ViewCount");
        Text scoreVews = new Text();
        
        
        //final IntWritable pair = new IntWritable();
        
        //String title = mapping.get("Title");
        
        
      	  //if(userId != null)
      	  //{
      		//if(score != null && vews != null)
      		//{
      			//id.set(userId);
      			//length.set(title.length());
      			scoreVews.set(score+","+vews);
        		context.write(one, scoreVews);
      		//}
      		  
      	 // }
        
		}
	}
		
		public static class reducer
	       extends Reducer<Text,Text,Text,DoubleWritable> {
	    private DoubleWritable result = new DoubleWritable();
	    private Text text = new Text("Cor-relation Coefficient: ");

	    public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException, IndexOutOfBoundsException {
	      
	      List<Integer> score = new ArrayList<>();
	      List<Integer> vews = new ArrayList<>();
	      for (Text element: values)
	      {
	    	  String str = element.toString();
	    	  //if(str != null)
	    	  //{
	    		  String[] score_vew = str.split(",");
	    		  int scor = 0;
		    	  int vew = 0;
	    		  try
	    		  {
	    			  scor = Integer.parseInt(score_vew[0].trim());
			    	  vew = Integer.parseInt(score_vew[1].trim());
			    	  score.add(scor);
			    	  vews.add(vew);
			    	  
	    		}catch(NumberFormatException nfe)
	    		{
	    			 //System.out.println(scor+" "+vew);
	    			 continue;
	    		}
		    	  
	    	  }
	      
	      Integer[] x = score.toArray(new Integer[0]);
		  Integer[] y = vews.toArray(new Integer[0]);
		  
		  Double corr_coff = correlaton_coefficient(x, y);
		  //if(!corr_coff.isNaN())
		  //{
			  result.set(corr_coff);
		      context.write(text, result);
		 // }
	      
	    }

		private double correlaton_coefficient(Integer[] xs, Integer[] ys) {
			// TODO Auto-generated method stub
			
			double sum_of_x = 0.0;
		    double sum_of_y = 0.0;
		    double sum_of_square_x = 0.0;
		    double sum_of_square_y  = 0.0;
		    double sum_of_square_xy  = 0.0;

		    int n = xs.length;

		    for(int i = 0; i < n; ++i) {
		      double x = xs[i];
		      double y = ys[i];

		      sum_of_x += x;
		      sum_of_y += y;
		      sum_of_square_x += x * x;
		      sum_of_square_y += y * y;
		      sum_of_square_xy += x * y;
		    }

		    // covariation
		    double cov = sum_of_square_xy / n - sum_of_x * sum_of_y / n / n;
		    //System.out.println(cov);
		    // standard error of x
		    double standard_error_x = Math.sqrt(sum_of_square_x / n -  sum_of_x * sum_of_x / n / n);
		    // standard error of y
		    //System.out.println(standard_error_x);
		    double standard_error_y = Math.sqrt(sum_of_square_y / n -  sum_of_y * sum_of_y / n / n);
		    //System.out.println(standard_error_y);
		    // correlation is just a normalized covariation
		    return cov / standard_error_x / standard_error_y;
		}
	  }
	  @Override
	  public int run(String[] args) throws Exception
	  {
		    Configuration conf = getConf();
		    
		    Job job = Job.getInstance(conf, "task2c");
		    job.setJarByClass(task2c.class);
		    job.setMapperClass(Map.class);
		    job.setReducerClass(reducer.class);
		    job.setOutputKeyClass(Text.class);
		    //job.setOutputValueClass(IntWritable.class);
		    job.setOutputValueClass(DoubleWritable.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    return job.waitForCompletion(true)?0:1;
	  }
	    public static void main(String[] args) throws Exception 
	    {
	    	ToolRunner.run(new Configuration(),new task2c(), args);
	    }
}


