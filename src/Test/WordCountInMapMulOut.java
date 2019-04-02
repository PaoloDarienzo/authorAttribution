package Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import authorAttribution.ArrayWritable;

//import org.apache.log4j.Logger;

public class WordCountInMapMulOut extends Configured implements Tool {

	//private static final Logger LOG = Logger.getLogger(WordCountInMap.class);

	public static void main(String[] args) throws Exception {
		  
		int res = ToolRunner.run(new WordCountInMapMulOut(), args);
		System.exit(res);
	}
	
	public int run(String[] args) throws Exception {
		  
		//wordcountinmap e' l'etichetta del job: compare nell'interfaccia web
		Job job = Job.getInstance(getConf(), "Word count in map with multiple output files (old)");
		job.setJarByClass(this.getClass());
		
		// Use TextInputFormat, the default unless job.setInputFormatClass is used
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		//job.setPartitionerClass(authorPartitioner.class);
		//job.setGroupingComparatorClass(GroupComparator.class);
		//job.setSortComparatorClass(Text.Comparator.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ArrayWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		//job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ArrayWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	/*
	public static class authorPartitioner extends Partitioner<Text, ArrayWritable> {
		
		@Override
		public int getPartition(Text key, ArrayWritable value, int numPartitions) {
			
			return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
			
		}
	}
	*/
	
	public static class Map extends Mapper<LongWritable, Text, Text, ArrayWritable> {
		
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		
		//private Text currentWord = new Text();
		//private IntWritable currentValue = new IntWritable();
		
		private Text author = new Text();
		private ArrayWritable H = new ArrayWritable();
		private List<String> conjuction = new ArrayList<>();

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			
			//. , ? ! " ( ) - : ;
			conjuction.add(".");
			conjuction.add(",");
			conjuction.add(":");
			conjuction.add(";");
			conjuction.add("?");
			conjuction.add("!");
			conjuction.add("(");
			conjuction.add(")");
			conjuction.add("-");
			conjuction.add("\"");
			
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			String delimiters = ",___,";
			String[] tokensVal = filename.split(delimiters);
			author.set(tokensVal[0]);
			
			String line = lineText.toString();
						
			for (String word : WORD_BOUNDARY.split(line)) {
				//skip is word is empty
				//or is if is not a digit or a char
				//and is not a conjuction symbol
				if (word.isEmpty() 
						|| (
							!(word.matches("^[a-zA-Z0-9]*$")) && !(conjuction.contains(word))
							) 
					) {
					continue;
				}
				H.increment(word.toLowerCase());		        
			}
			
			context.write(author, H);
			
		}
	
	}

	//Text, ArrayWritable, Text, ArrayWritable
	public static class Reduce extends Reducer<Text, ArrayWritable, NullWritable, ArrayWritable> {
	
		private MultipleOutputs<NullWritable, ArrayWritable> multipleOutputs;
		
		public void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<NullWritable, ArrayWritable>(context);
		}
		
		@Override
		public void reduce(Text author, Iterable<ArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			
			ArrayWritable final_array = new ArrayWritable();
			
			for(ArrayWritable value : values) {
				final_array.sum(value);
			}
			
			multipleOutputs.write(NullWritable.get(), final_array, author.toString());
			//context.write(author, final_array);	
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}
		
	}
	
}
