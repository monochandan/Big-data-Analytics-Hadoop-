import java.io.IOException;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class task1b extends Configured implements Tool{
	public static class Map extends Mapper<Object, Text, Text, Text> {
		private Text name = new Text();
		private Text block = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String input = value.toString().replace("# OLD_NAME", "").replace(" NEW_NAME", "").trim();
			//String input = value.toString();
			String[] names = input.split(",");
			
			if (names.length != 2)
				return;
			
			for(String element: names)
			{
				name.set(element);
				String[] words = element.split(" ");
				String from_first_word = words[0].trim().substring(0, 1);// first letter of the first name
				String from_last_word;
				if(words[words.length - 1].length()< 3)
					from_last_word = (String) words[words.length - 1].trim();
				else
					from_last_word = (String) words[words.length - 1].trim().substring(0, 3); // first three letters of the last word
				String new_name = from_first_word + from_last_word;
				
				
				if(element.equals("Renée J. Miller"))
					//System.out.println(new_name);
				block.set(new_name);
				context.write(block, name);
				
			}
			
		}
		
		
	
	}
	
	public static class reducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		//private Text key = new Text();
		String temp;
		Set names = new HashSet<>();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			if (key.toString().compareTo("RMil") == 0) {
				for (Text element : values) {
					temp = element.toString();
					names.add(temp+"\n");
				}
			result.set(names.toString());
			context.write(key, result);
			}
			//System.out.println(names);

		}
	
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "task1b");
		job.setJarByClass(task1b.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		//job.output.getFileSystem(conf).delete(output,true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new task1b(), args);
	}

}
