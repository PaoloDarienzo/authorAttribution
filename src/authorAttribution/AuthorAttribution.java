package authorAttribution;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;

public class AuthorAttribution extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new AuthorAttribution(), args);
		System.exit(res);
		
	} //end main class
		
		public int run(String[] args) throws Exception {
						
			Job job = Job.getInstance(getConf(), "Author Attribution");
			job.setJarByClass(this.getClass());
			
			//FileInputFormat.setInputDirRecursive(job, true);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.setNumReduceTasks(Integer.parseInt(args[2]));
			
			job.setMapperClass(AuthorAttrMapper.class);
			job.setReducerClass(AuthorAttrReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			return job.waitForCompletion(true) ? 0 : 1;
	    
	  }
	
	public static class AuthorAttrMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private final static Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		
		private Text currentWord = new Text();
		
		@Override
		public void map(LongWritable offset, Text lineText, Context context) 
				throws IOException, InterruptedException {
			
			String line = lineText.toString();
			
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				word = word.toLowerCase();
				currentWord.set(word);
	            
				context.write(currentWord, one);
				
			}
			
		}//end map

	} //end AuthorAttrMapper class

	public static class AuthorAttrReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		//private MultipleOutputs<NullWritable, IntWritable> multipleOutputs;
		
		/*
		public void setup(Context context) throws IOException, InterruptedException
		{
			multipleOutputs = new MultipleOutputs<NullWritable, IntWritable>(context);
		}
		*/

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable count : values) {
				sum += count.get();
			}
			context.write(key, new IntWritable(sum));
			//multipleOutputs.write(NullWritable.get(), new IntWritable(sum), key.toString());
			
		}//end reduce
		
		/*
		public void cleanup(Context context) 
				throws IOException, InterruptedException {
			multipleOutputs.close();
		}		
		*/
		
	} //end AuthorAttrReducer class	
	
	/*
	public class AuthorAttributionPartitioner extends Partitioner<LongWritable, Text> {
		
		@Override
		public int getPartition(LongWritable key, Text value, int numPartitions) {
			return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}
	*/
	
} //end AuthorAttribution class