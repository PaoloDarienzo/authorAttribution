package test;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import authorAttribution.WordsArrayWritable;

//import org.apache.log4j.Logger;

public class WordCountInMemMulOut extends Configured implements Tool {

	//private static final Logger LOG = Logger.getLogger(WordCountInMap.class);

	public static void main(String[] args) throws Exception {
		  
		int res = ToolRunner.run(new WordCountInMemMulOut(), args);
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
		job.setMapOutputValueClass(WordsArrayWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(WordsArrayWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, WordsArrayWritable> {
		
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		
		//private Text currentWord = new Text();
		//private IntWritable currentValue = new IntWritable();
		
		private Text author = new Text();
		private WordsArrayWritable H = new WordsArrayWritable();

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

	public static class Reduce extends Reducer<Text, WordsArrayWritable, NullWritable, WordsArrayWritable> {
	
		private MultipleOutputs<NullWritable, WordsArrayWritable> multipleOutputs;
		
		public void setup(Context context) throws IOException, InterruptedException
		{
			multipleOutputs = new MultipleOutputs<NullWritable, WordsArrayWritable>(context);
		}
		
		@Override
		public void reduce(Text word, Iterable<WordsArrayWritable> counts, Context context)
				throws IOException, InterruptedException {
			
			WordsArrayWritable Hf = new WordsArrayWritable();
			
			for(WordsArrayWritable value : counts) {
				Hf.sum(value);
			}
			multipleOutputs.write(NullWritable.get(), Hf, word.toString());
			//context.write(word, Hf);	
		}
		
		public void cleanup(Context context) 
				throws IOException, InterruptedException {
			multipleOutputs.close();
		}	
		
	}
	
}