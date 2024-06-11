import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.apache.hadoop.shaded.org.apache.commons.io.FileUtils;
import org.apache.hadoop.shaded.org.apache.curator.shaded.com.google.common.io.Files;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import java.nio.file.Path;
//import java.nio.file.Files;
import java.io.BufferedReader;
import java.io.DataInputStream;

import mrdp.utils.MRDPUtils;

public class assignment_5_task_1b extends Configured implements Tool {
	
	 //Collection<Object> set = Stream.of().collect(Collectors.toCollection(HashSet::new));
	
		static Set<String>set = new HashSet<>();
	 
	 public static class Mapsidejoin extends Mapper<Object, Text, Text, NullWritable>{
		 
		    
		 	private static Text ID = new Text();
		    private static NullWritable rslt = NullWritable.get();
		    
		    public void map(Object key, Text value, Context context
		                    ) throws IOException, InterruptedException 
		    {
		    	String text = value.toString();
		    	
		    	Map<String, String> mapping = MRDPUtils.transformXmlToMap(text);
		    	
		    	String user_id = mapping.get("OwnerUserId");
		    	
		    	try {
					if ((user_id != null)&&(set.contains(user_id))&&((Integer.valueOf(user_id)>=110)&&(Integer.valueOf(user_id)<=120)))
					{
						String id = mapping.get("Id");
						ID.set("User Id:"+user_id+" ;Id:"+id+"");
						context.write(ID,rslt);
					}
				} catch (NumberFormatException | IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    	
		    	
		    }
		    
		    
	 }
	
	 public assignment_5_task_1b(Object file) throws IOException {
	  	
		   //String path="D:\\file\\users.xml";
		   //System.out.println("path doesnot exist");
		   //List<String> lines= FileUtils.readLines(new File(path));
		   //Path fileName = Path.of("D:\\file\\users.xml");
		   //String str = Files.readString((java.nio.file.Path) fileName);
		    //List all_lines = new ArrayList();
		 	//File xml_file = new File("D:\\file\\users.xml");
		 	
		     String xml_file = "D:\\file\\users.xml";
		 	/*Reader fileReader = new FileReader(xml_file);
		 	BufferedReader bufReader = new BufferedReader(fileReader);
		 	StringBuilder sb = new StringBuilder();
		 	String lines = bufReader.readLine();
		 	String temp;
		 	int valid_id = 0;
		 	String[]splitter;
		 	System.out.println("print");
		 	//lines = bufReader.readLine();*/
		 	 StringBuilder sb = new StringBuilder();
		 	 FileInputStream fstream = new FileInputStream(xml_file);
             DataInputStream in = new DataInputStream(fstream);
             BufferedReader br = new BufferedReader(new InputStreamReader(in));
             String lines;
             
             while((lines=br.readLine()) != null)
	  		{
	  			
	  			
	  			//sb.append(lines).append("\n");
	  			//System.out.println(sb);
	  			//all_lines.add(sb.toString());
	  			sb.append(lines).append("\n");
	  			//System.out.println("lines are "+lines);
	  			Map<String, String> map=MRDPUtils.transformXmlToMap(lines);
	  			String id=map.get("Id");
	  			//System.out.println("ID are "+id);
	  			set.add(id);
	  			
	  	    }
	  		//String xml2String = sb.toString();
	  		//System.out.println(xml2String);
	  		
	         
	  		//br.close();
	    }
		    
  @Override
  public int run(String[] args) throws Exception
  {
	    Configuration conf = getConf();
	    
	    Job job = Job.getInstance(conf, "map-side join");
	    job.setJarByClass(assignment_5_task_1b.class);
	    job.setMapperClass(Mapsidejoin.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    job.setNumReduceTasks(0);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    return job.waitForCompletion(true)?0:1;
  }
    public static void main(String[] args) throws Exception 
    {
    	ToolRunner.run(new Configuration(),new assignment_5_task_1b(args), args);
    }
}