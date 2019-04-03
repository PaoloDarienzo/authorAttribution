package Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;

public class AuthorAttributionPair extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new AuthorAttributionPair(), args);
		System.exit(res);
		
	} //end main class
		
	public int run(String[] args) throws Exception {
					
		Job job = Job.getInstance(getConf(), "Author Attribution");
		job.setJarByClass(this.getClass());
		
		//FileInputFormat.setInputDirRecursive(job, true);
		
		//input e output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//number of reducers
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		
		//Avoid creating empty files (lazy output)
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(authorPartitioner.class);
		//job.setGroupingComparatorClass(GroupComparator.class);
		//job.setSortComparatorClass(Text.Comparator.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
		
	}
		
	public static class authorPartitioner extends Partitioner<TextPair, IntWritable> {
		
		@Override
		public int getPartition(TextPair key, IntWritable value, int numPartitions) {
			
			return (key.getFirst().hashCode() * 163 & Integer.MAX_VALUE) % numPartitions;
			
		}
	}
	
	public static class Map extends Mapper<LongWritable, Text, TextPair, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private final static Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		private final static String DELIMITERS = ",___,";		
		//. , : ; ? ! ( ) - "
		public static final String[] SET_CONJ_VALUES = new String[] {	".", ",", ":", ";",
																		"?", "!", "(", ")",
																		"-", "\""};
		public static final Set<String> CONJUCTION = new HashSet<>(Arrays.asList(SET_CONJ_VALUES));
		
		private TextPair currentPair = new TextPair();
		
		//private TraceWord currentTraceWord = new TraceWord();
		//private Text currentWord = new Text();
		private Text author = new Text();
		
		@Override
		public void map(LongWritable offset, Text lineText, Context context) 
				throws IOException, InterruptedException {
			
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			String[] tokensVal = filename.split(DELIMITERS);
			author.set(tokensVal[0]);
			
			//currentPair.setFirst(author);
			
			currentPair.setFirst(author);
			
			String line = lineText.toString();
			
			for (String word : WORD_BOUNDARY.split(line)) {
				//skip is word is empty
				//or is if is not a digit or a char
				//and is not a conjuction symbol
				if (word.isEmpty() 
						|| (
							!(word.matches("^[a-zA-Z0-9]*$")) && !(CONJUCTION.contains(word))
							) 
					) {
					continue;
				}
								
				//currentWord.set(word.toLowerCase());
				currentPair.setSecond(word.toLowerCase());
				//currentTraceWord.setWord(word.toLowerCase());
	            
				context.write(currentPair, one);
				
			} //end for
			
		}//end map

	} //end Mapper class

	public static class Reduce extends Reducer<TextPair, IntWritable, Text, IntWritable>{

		private MultipleOutputs<Text, IntWritable> multipleOutputs;
		
		public void setup(Context context) throws IOException, InterruptedException
		{
			multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
		}

		@Override
		public void reduce(TextPair key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable count : values) {
				sum += count.get();
			}
			
			//context.write(key, new IntWritable(sum));
			multipleOutputs.write(key.getSecond(), new IntWritable(sum), key.getFirstToString());
			
		}//end reduce
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}		
		
	} //end Reducer class	
	
} //end AuthorAttribution class