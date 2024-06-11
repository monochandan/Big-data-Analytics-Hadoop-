import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

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

public class Assignment5Task2 extends Configured implements Tool {

	final static int t = 1000;

	public static class MyMap extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text ngram = new Text();
		
		
	    public static List<String> ngrams(int n, String str) {
	        List<String> ngrams = new ArrayList<String>();
	        String[] words = str.split(" ");
	        for (int i = 0; i < words.length - n + 1; i++)
	            ngrams.add(concat(words, i, i+n));
	        return ngrams;
	    }

	    public static String concat(String[] words, int start, int end) {
	        StringBuilder sb = new StringBuilder();
	        for (int i = start; i < end; i++)
	            sb.append((i > start ? " " : "") + words[i]);
	        return sb.toString();
	    }
	    public String[] splitDelimitedStr(String str, String delimiter) {
	    	  Pattern pttn = Pattern.compile(delimiter);
	    	  return pttn.split(str);
	    	}
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
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
			List<String> list = new ArrayList<>();
			
	        for (int n = 2; n <= 4; n++) {
	            for (String ngram : ngrams(n, input)) {
	            	list.add(ngram.toString());
	            }
	        }

	        String[] sarray = null;
	        for (String s : list) {
				sarray = splitDelimitedStr(s,",");
		        StringBuilder builder = new StringBuilder();  
		        for (int i=0; i<sarray.length; i++){  
		        	builder.append(sarray[i]);  
		        } 
				String str = builder.toString();
				ngram.set(str);
				context.write(ngram, one);
			}
		}
	}

	public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values)
				count += val.get();
			if (count < t)
				return;
			result.set(count);
			context.write(key, result);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "usercounter");
		job.setJarByClass(Assignment5Task2.class);
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new Assignment5Task2(), args);
	}
}