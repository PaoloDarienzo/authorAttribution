package authorAttribution;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class WordCountAuth extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
	  
		int res = ToolRunner.run(new WordCount(), args);
		System.exit(res);
		}

	public int run(String[] args) throws Exception {
	  
		Job job = Job.getInstance(getConf(), "WordCountAuth");
		job.setJarByClass(this.getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TextPair.class);
		/*
		setOutputKeyClass e setOutputValue class
		controllano il tipo di output di map e reduce;
		se questi tipi sono diversi, utilizzo i metodi:
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ArrayWritable.class);
		
		Tipo input di default è TextInputFormat
		*/
		
		return job.waitForCompletion(true) ? 0 : 1;
    
	}

	public static class Map extends Mapper<LongWritable, Text, TextPair, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private final static Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		
		private Text currentWord = new Text();
		private Text author = new Text();
		private TextPair authWord = new TextPair();

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			String delimiters = ",___,";
			String[] tokensVal = filename.split(delimiters);
			author.set(tokensVal[0]);
			
			authWord.setFirst(author);
			
			String line = lineText.toString();
			
			for (String word : WORD_BOUNDARY.split(line)) {
				//skip spaces, and not alphanumerical characters
				if (word.isEmpty() || !(word.matches("^[a-zA-Z0-9]*$"))) {
					continue;
				}
				//No differences in lower or uppercase
				word = word.toLowerCase();
				currentWord.set(word);
				
				authWord.setSecond(currentWord);
				context.write(authWord, one);
			}
		}
 
	}

	public static class Reduce extends Reducer<TextPair, IntWritable, Text, TextPair> {
	
		@Override
		public void reduce(TextPair authWord, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			
			TextPair wordVal = new TextPair(authWord.getSecond(), new Integer(sum).toString());
			context.write(authWord.getFirst(), wordVal);
		}
	}

}
