import java.io.IOException;



import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import java.util.Map;
import mrdp.utils.*;


public class assignment_5_task1a extends Configured implements Tool {
	
			private final static Text keys = new Text();
			private final static Text values = new Text();
			
			public static class UsersMap extends Mapper<Object, Text, Text, Text>
			{
				//private final static IntWritable length = new IntWritable(1);
				//private Text id = new Text();
				
				public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	String input=value.toString();
	    	
	    	HashMap<String, String> mapping = new HashMap<String, String>();
	    	mapping = (HashMap<String, String>) MRDPUtils.transformXmlToMap(input);
	        
	        String userId = mapping.get("Id");
	        String up_votes = mapping.get("UpVotes");
	        String name = mapping.get("DisplayName");
	        
	        
	        //List ls = new ArrayList();
	        
	        if(userId != null)
	     	 {
	        	if(Integer.parseInt(userId) >= 20 && Integer.parseInt(userId) <= 40 )	
	        	{
	        		String str = "Id:"+userId+", UpVotes:"+ up_votes +", Name:"+ name;
	        		keys.set(userId);
	     			//values.set(input);
	     			//System.out.println("user Class:keys "+ keys);
	      			//System.out.println("user Class:values "+ str);
	     			context.write(keys, new Text("Post_Id:"+userId+", UpVotes:"+ up_votes +", Name:"+ name));
	        	}
	     			
	     	 }
	        
			}
		}
		
	
		public static class PostMap extends Mapper<Object, Text, Text, Text>
		{
			//private final static IntWritable post = new IntWritable(1);
			
			
			public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
				String input = value.toString();
				//String[] line = input.split(",");
    	
    	HashMap<String, String> mapping = new HashMap<String, String>();
    	mapping = (HashMap<String, String>) MRDPUtils.transformXmlToMap(input);
    	
    	//String line = mapping.toString();
    	String user_Id = mapping.get("OwnerUserId");// key --- ID
        String score = mapping.get("Score"); // value --- Score
        //String title = mapping.get("Title");
        
        //userId=mapping.get("Id"); // map side join
    	if (user_Id != null)
      	 {
 
      			keys.set(user_Id);
      			//values.set("score "+score); //new Text(" Score:  "+score)
      			if(Integer.parseInt(user_Id) >= 20 && Integer.parseInt(user_Id) <= 40 )
      			{
      	   			//System.out.println("Post Class:keys "+ keys);
          			//System.out.println("Post Class:values "+ score);
            		context.write(keys, new Text("User_ID:"+user_Id+", Score:"+score));
      			}
   
      	 }
        
		}
	}
		
			
		public static class reducer
	       extends Reducer<Text,Text,Text,Text> {
	   // private DoubleWritable result = new DoubleWritable();
			 private Text result = new Text();
	    public void reduce(Text key, Iterable<Text> values,
                			Context context) throws IOException, InterruptedException, IndexOutOfBoundsException,NumberFormatException {
	     
	    	//String temp;
	   
		String[] temp;
		List elem = new ArrayList();
		String txt = null;
		Double avg_score;
		Double post = 0.0;
		int upvotes = 0;
		int count = 0;
		int user_id = 0;
		int post_id = 0;
		int u_id = 0;
		Double score = 0.0;
		Double average_score = 0.0;
		String name = " ";
		String[]splitter;
		for(Text element: values)
		{
			String[] parts = element.toString().split(",");
			//"Id:"+userId+", UpVotes:"+ up_votes +", Name:"+ name "Score:"+score
			for(String i : parts)
			{
				//elem.add(i);
				if(i.contains("User_ID"));
				{
					//splitter = i.split(":");
					//u_id = Integer.parseInt(splitter[1]);
					if(i.contains("Name"))
					{
						splitter = i.split(":");
						name = splitter[1];
					}
					if(i.contains("Score"))
					{
						splitter = i.split(":");
						score += Float.parseFloat(splitter[1]);
						count++;
					}
					if(i.contains("UpVotes"))
					{
						splitter = i.split(":");
						upvotes += Integer.parseInt(splitter[1]);
					}
				}
			}
			
				
		}
	
				  
		//result.set(elem.toString());
		//String str = "post id: "+post_id+", User id:"+u_id+", Total Post: "+count+", Score: "+score+", Average Score: "+(score/count)+", Up votes: "+upvotes;
		//System.out.println("reducer Class:keys "+ name);
		//System.out.println("reduicer Class:values "+ str);
		average_score = score / count;
		
		//new Text("Total Post: "+count+" Score: "+score+" Average Score: "+(score/count)+" Up votes: "+upvotes)
  	  	if(!Double.isNaN(average_score))
  	  		context.write(new Text(name), new Text("Total Post: "+count+" Score: "+score+" Average Score: "+average_score+" Up votes: "+upvotes));	
	      //context.write(key, result);  
	   
	  }
		}
	  @Override
	  public int run(String[] args) throws Exception
	  {
		    Configuration conf = getConf();
		    
		    Job job = Job.getInstance(conf, "assignment_5_task1a");
		    job.setJarByClass(assignment_5_task1a.class);
		    //job.setMapperClass(PostMap.class);
		    //job.setMapperClass(UsersMap.class);
		    job.setReducerClass(reducer.class);
		    job.setOutputKeyClass(Text.class);
		    //job.setOutputValueClass(IntWritable.class);
		    //job.setOutputValueClass(DoubleWritable.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    //FileInputFormat.addInputPath(job, new Path(args[0]));
		    
		    MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class,UsersMap.class);
		    MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class,PostMap.class);
		    
		    FileOutputFormat.setOutputPath(job, new Path(args[2]));
		    return job.waitForCompletion(true)?0:1;
	  }
	    //@SuppressWarnings("deprecation")
		public static void main(String[] args) throws Exception 
	    {
	    	ToolRunner.run(new Configuration(),new assignment_5_task1a(), args);
	    	
	    	
	    }
		/*@Override
		public int run(String[] arg0) throws Exception {
			// TODO Auto-generated method stub
			return 0;
		}*/
	    
}

