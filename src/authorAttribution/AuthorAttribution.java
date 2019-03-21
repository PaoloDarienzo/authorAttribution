package authorAttribution;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

/**
 * 
 * @author paolo
 *
 */
public class AuthorAttribution extends Configured implements Tool {

	/**
	 * 
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new AuthorAttribution(), args);
		System.exit(res);
		
	} //end main class
		
		public int run(String[] args) throws Exception {
			
			Configuration conf = new Configuration();
			//to read input directories recursively
			conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
			
			Job job = Job.getInstance(getConf(), "Author Attribution");
			job.setJarByClass(this.getClass());
			
			Path inputPath = new Path(args[0]);
			Path outputPath = new Path(args[1]);
			FileInputFormat.addInputPath(job, inputPath);
			FileOutputFormat.setOutputPath(job, outputPath);
			
			job.setNumReduceTasks(Integer.parseInt(args[2]));
			
			job.setMapperClass(AuthorAttrMapper.class);
			job.setReducerClass(AuthorAttrReducer.class);
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(IntWritable.class);
			
			return job.waitForCompletion(true) ? 0 : 1;
	    
	  }
	
	/**
	 * 
	 * @author paolo
	 *
	 */
	public static class AuthorAttrMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private final static Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		private Text currentWord = new Text();
		
		/**
		 * @throws InterruptedException 
		 * @throws IOException 
		 * 
		 */
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			String line = value.toString();
			
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				word.toLowerCase();
				currentWord.set(word);
	            
				context.write(currentWord, one);
				
			}
			
		}//end map

	} //end AuthorAttrMapper class

	/**
	 * 
	 * @author paolo
	 *
	 */
	public static class AuthorAttrReducer extends Reducer<Text, IntWritable, NullWritable, IntWritable>{

		private MultipleOutputs<NullWritable, IntWritable> multipleOutputs;
		
		public void setup(Context context) throws IOException, InterruptedException
		{
			multipleOutputs = new MultipleOutputs<NullWritable, IntWritable>(context);
		}
		
		/**
		 * @throws InterruptedException 
		 * @throws IOException 
		 * 
		 */
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable count : values) {
				sum += count.get();
			}
			multipleOutputs.write(NullWritable.get(), new IntWritable(sum), key.toString());
			
		}//end reduce
								
		public void cleanup(Context context) 
				throws IOException, InterruptedException {
			multipleOutputs.close();
		}		
		
	} //end AuthorAttrReducer class	
	
	public class AuthorAttributionPartitioner extends Partitioner<LongWritable, Text> {
		
		@Override
		public int getPartition(LongWritable key, Text value, int numPartitions) {
			return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}
	
} //end AuthorAttribution class