package search;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import support.*;

public class AuthorAttributionSearch extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new AuthorAttributionSearch(), args);
		System.exit(res);
		
	} //end main class
	
	public int run(String[] args) throws Exception {
		
		Path out = new Path(args[1]);
		
		Job creation = Job.getInstance(getConf(), "Creation footprint of unknown file");
		creation.setJarByClass(this.getClass());
		
		FileInputFormat.addInputPath(creation, new Path(args[0]));
		FileOutputFormat.setOutputPath(creation, new Path(out, "creation"));
		LazyOutputFormat.setOutputFormatClass(creation, TextOutputFormat.class);
		
		//Inserted as default; 1 reducer per input file to test
		creation.setNumReduceTasks(1);
		
		creation.setMapperClass(creation.AuthorAttributionCreation.Map.class);
		creation.setReducerClass(creation.AuthorAttributionCreation.Reduce.class);
		
		creation.setMapOutputKeyClass(Text.class);
		creation.setMapOutputValueClass(BookTrace.class);
		creation.setOutputKeyClass(NullWritable.class);
		creation.setOutputValueClass(AuthorTrace.class);
		
		creation.setPartitionerClass(creation.AuthorAttributionCreation.authorPartitioner.class);
		
		//creation.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		if (!creation.waitForCompletion(true)) {
		  System.exit(1);
		}
		
		///////////////////////////////////////////////////////////////////////////////////////////
		
		Job search = Job.getInstance(getConf(), "Search for best match");
		search.setJarByClass(this.getClass());
		
		getConf().set("textinputformat.record.delimiter","#####");
		search.setInputFormatClass(TextInputFormat.class);
		
		search.setMapperClass(Map.class);
		//TODO
		//search.setReducerClass(Reduce.class);
		
		search.setOutputKeyClass(NullWritable.class);
		search.setOutputValueClass(Text.class);
		
		//TODO
		search.setNumReduceTasks(0);
		
		//search.setSortComparatorClass(LongWritable.DecreasingComparator.class);		
		//search.setInputFormatClass(SequenceFileInputFormat.class);
		
		FileInputFormat.addInputPath(search, new Path(out, "creation"));
		FileOutputFormat.setOutputPath(search, new Path(out, "result"));
		
		if (!search.waitForCompletion(true)) {
		  System.exit(1);
		}
		
		return 0;
				
	}
	
	public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> {
			
		private String author, numWords, totChars, funcWords, punctWords, noCouples, noTrigrams;
		private HashMap<String, Integer> wordCount;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			author = numWords = totChars = funcWords = punctWords = noCouples = noTrigrams = "";
			wordCount = new HashMap<>();
		}
		
		@Override
		public void map(LongWritable offset, Text lineText, Context context) 
				throws IOException, InterruptedException {
						
			//lineText is from separator to separator
			String line = lineText.toString();
			
			if(line.contains("@Autore: ")) {
				String boundary = "@Autore: ";
				String[] tokensVal = line.split(boundary);
				author = tokensVal[1];
			}
			else if(line.contains("Num words: ")) {
				String boundary = "Num words: ";
				String[] tokensVal = line.split(boundary);
				numWords = tokensVal[1];
			}
			else if(line.contains("@Num total chars: ")) {
				String boundary = "@Num total chars: ";
				String[] tokensVal = line.split(boundary);
				totChars = tokensVal[1];
			}
			else if(line.contains("@Num function words: ")) {
				String boundary = "@Num function words: ";
				String[] tokensVal = line.split(boundary);
				funcWords = tokensVal[1];
			}
			else if(line.contains("@Num punctuation words: ")) {
				String boundary = "@Num punctuation words: ";
				String[] tokensVal = line.split(boundary);
				punctWords = tokensVal[1];
			}
			else if(line.contains("@Num couples: ")) {
				String boundary = "@Num couples: ";
				String[] tokensVal = line.split(boundary);
				noCouples = tokensVal[1];
			}
			else if(line.contains("@Num trigrams: ")) {
				String boundary = "@Num trigrams: ";
				String[] tokensVal = line.split(boundary);
				noTrigrams = tokensVal[1];
			}
			else if(line.contains("@WordCount: ")) {
				String boundary = "@WordCount: ";
				String[] tokensVal = line.split(boundary);
				for(String word : tokensVal[1].split(", ")) {
					String[] entry = word.split("=");
					String key = entry[0];
					Integer value = new Integer(entry[1]);
					wordCount.put(key, value);
				}
			}
			
		}//end map
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			String toEmit = "Author: " + author +
					"\nNumWords: " + numWords + 
					"\nTotChars: " + totChars +
					"\nFuncWords: " + funcWords +
					"\nPunctWords: " + punctWords +
					"\nCouples: " + noCouples +
					"\nTrigrams: " + noTrigrams + "\n" + 
					"\nWordCount: \n" + wordCount.toString() + "\n";
			
			context.write(NullWritable.get(), new Text(toEmit));
			
		}

	} //end Mapper class

	/*
	 * Each reducer gets all jobs relatively to an author, thanks to the partitioner
	 */
	public static class Reduce extends Reducer<Text, BookTrace, NullWritable, AuthorTrace> {
		
		/*
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			
		}
		*/
		
		@Override
		public void reduce(Text key, Iterable<BookTrace> values, Context context) 
				throws IOException, InterruptedException {
			
			
		}//end reduce
		
		/*
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			
		}	
		*/
		
	} //end Reducer class	

}
