package Test;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import authorAttribution.ArrayWritable;

//import org.apache.log4j.Logger;

public class WordCountInMap extends Configured implements Tool {

	//private static final Logger LOG = Logger.getLogger(WordCountInMap.class);

	public static void main(String[] args) throws Exception {
		  
		int res = ToolRunner.run(new WordCountInMap(), args);
		System.exit(res);
	}
	
	public int run(String[] args) throws Exception {
		  
		//wordcountinmap e' l'etichetta del job: compare nell'interfaccia web
		Job job = Job.getInstance(getConf(), "wordcountinmap");
		job.setJarByClass(this.getClass());
		
		// Use TextInputFormat, the default unless job.setInputFormatClass is used
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ArrayWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, ArrayWritable> {
		
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		
		//private Text currentWord = new Text();
		//private IntWritable currentValue = new IntWritable();
		
		private Text author = new Text();
		private ArrayWritable H = new ArrayWritable();

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			String delimiters = ",___,";
			String[] tokensVal = filename.split(delimiters);
			author.set(tokensVal[0]);
			
			String line = lineText.toString();
						
			for (String word : WORD_BOUNDARY.split(line)) {
				//skip spaces, and not alphanumerical characters
				if (word.isEmpty() || !(word.matches("^[a-zA-Z0-9]*$"))) {
					continue;
				}
				//word = word.toLowerCase();
				H.increment(word.toLowerCase());
		        
			}
			
			context.write(author, H);
			
		}
	
	}

	public static class Reduce extends Reducer<Text, ArrayWritable, Text, ArrayWritable> {
	
		@Override
		public void reduce(Text word, Iterable<ArrayWritable> counts, Context context)
				throws IOException, InterruptedException {
			
			ArrayWritable Hf = new ArrayWritable();
			
			for(ArrayWritable value : counts) {
				Hf.sum(value);
			}
			
			context.write(word, Hf);
			
			Hf.clear();
			
		}
	}
	
}
