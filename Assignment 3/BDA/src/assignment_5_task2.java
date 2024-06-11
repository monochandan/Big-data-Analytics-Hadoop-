import java.io.IOException;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.lang.model.SourceVersion;

import java.lang.StringBuilder;
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
import org.apache.hadoop.shaded.com.google.re2j.Pattern;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class assignment_5_task2 extends Configured implements Tool{
	final static int t = 1000;
	public static class map extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private final static Text ngram = new Text();
		
		private List<String> ngrams(int n, String input) {
			// TODO Auto-generated method stub
			List<String> ngram = new ArrayList<>();
			//String[] words = input.split(" ");
			List<String>words = new ArrayList<>();
			
			for(int i = 0; i< words.size()-n+1; i++)
			{
				ngram.add(String.valueOf(words.subList(i,  i+n)));
				System.out.println("ngram():"+ngram);
			}
			
			return ngram;
		}
		
		// create pattern from each element of the list delimited by comma 
		private String[] splitDelimitedStr(String s, String string) {
			// TODO Auto-generated method stub
			//StringBuilder sb = new StringBuilder();
			Pattern ptrn = Pattern.compile(string);
			System.out.println("pattern():"+ptrn);
			
			return ptrn.split(s);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String input = value.toString().replace('\"', ' ')
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
			
			//  computes n_grams and add to the list
			List<String> list = new ArrayList<>();
			
			for(int n = 2; n<= 4; n++)
			{
				for(String ngram:ngrams(n, input))
				{
					list.add(ngram);
				}
			}
			
			//every element of list will be converted to string
			/*String temp;
			for(String s: list)
			{
				temp = s.replace(",", "");
				ngram.set(str);
			}*/
			
			String[] sarray = null;
			for(String s: list)
			{
				sarray = splitDelimitedStr(s, ",");
				// build string from the every pattern from the splitDelimitedStr function 
				StringBuilder builder = new StringBuilder();
				for(int i = 0; i<sarray.length; i++)
				{
					// in builder we are adding every words of element s from the sarray; 
					builder.append(sarray[i]);
					System.out.println("Builder:"+builder);
				}
				String str = builder.toString();
				ngram.set(str);
				context.write(ngram, one);
				
			}
			
		}
		
		
	}
	
	public static class reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int count = 0;
			for(IntWritable v: values)
			{
			  count += v.get();
			}
			if(count >= t)
				result.set(count);
				context.write(key, result);
		}
		
	}
	public int run(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
		//output.getFileSystem(conf).delete(output,true);
		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf, "N-Gram");
		job.setJarByClass(assignment_5_task2.class);
		
		job.setMapperClass(map.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true)? 0 : 1;
		
	}

	
	public static void main(String[] args) throws Exception
	{
		ToolRunner.run(new Configuration(), new assignment_5_task2(), args);
	}

}
