package search;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import creation.AuthorAttributionCreation.authorPartitioner;
import support.*;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;

public class AuthorAttributionSearch extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new AuthorAttributionSearch(), args);
		System.exit(res);
		
	} //end main class
	
	public int run(String[] args) throws Exception {
		
		//arg0: path to input file to analyze
		//arg1: path to output directory
		//arg2: number of reducers (same as number of authors)
		//arg3: path to author-known files to compare with
		
		Path inPath = new Path(args[0]);
		Path outPath = new Path(args[1]);
		Path creationPath = new Path(outPath, "creation");
		Path resultPath = new Path(outPath, "result");
		//TODO
		Path toMatchPath = new Path("/user/paolo/authorAttr/output/creation/veryshort");
		//Path toMatchPath = new Path(args[3]);
		
		//JOB 1: Creation profile of unknown file
		///////////////////////////////////////////////////////////////////////////////////////////

		Job creation = Job.getInstance(getConf(), "1. Creation profile of unknown file");
		creation.setJarByClass(this.getClass());
		
		FileInputFormat.addInputPath(creation, inPath);
		FileOutputFormat.setOutputPath(creation, creationPath);
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
		
		if (!creation.waitForCompletion(true)) {
		  System.exit(1);
		}
		
		//JOB 2: Merging profiles and generating similarity percentage
		///////////////////////////////////////////////////////////////////////////////////////////
		
		Job search = Job.getInstance(getConf(), "2. Merging and searching");
		search.setJarByClass(this.getClass());
		
		MultipleInputs.addInputPath(search, creationPath, TextInputFormat.class);
		MultipleInputs.addInputPath(search, toMatchPath, TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(search, resultPath);
		search.setNumReduceTasks(Integer.parseInt(args[2]));
		
		search.setMapperClass(Map.class);
		search.setReducerClass(Reduce.class);
		
		search.setMapOutputKeyClass(AuthorTrace.class);
		search.setMapOutputValueClass(IntWritable.class);
		//TODO
		search.setOutputKeyClass(NullWritable.class);
		search.setOutputValueClass(Text.class);
		
		//search.setPartitionerClass(authorFieldPartitioner.class);
		
		return search.waitForCompletion(true) ? 0 : 1;
				
	}
	
	public static class Map extends Mapper<LongWritable, Text, AuthorTrace, IntWritable> {
		
		private final static Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		
		private static AuthorTrace authorTrace;
		
		//AuthorTrace fields
		private static String author;
		private static float avgWordLength;
		private static float punctuationDensity;
		private static float functionDensity;
		private static HashMap<String, Integer> wordCount;
		private static HashMap<TextPair, Integer> twoGrams;
		private static HashMap<TextTrigram, Integer> threeGrams;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
		
			authorTrace = new AuthorTrace();
			
			author = "";
			avgWordLength =	punctuationDensity = functionDensity = 0;
			wordCount = new HashMap<>();
			twoGrams = new HashMap<>();
			threeGrams = new HashMap<>();
			
		}
		
		@Override
		public void map(LongWritable offset, Text lineText, Context context) 
				throws IOException, InterruptedException {
			
			String line = lineText.toString();
			
			for (String word : WORD_BOUNDARY.split(line)) {
				//skip if word is empty
				if(word.isEmpty()) {
					continue;
				}
				
				if(line.contains("Author: ")) {
					String boundary = "Author: ";
					String[] tokensVal = line.split(boundary);
					author = tokensVal[1];
				}//overwritten in cleanup if file is unknown
				else if(line.contains("Avg word length: ")) {
					String boundary = "Avg word length: ";
					String[] tokensVal = line.split(boundary);
					String avgWordLengthStr = tokensVal[1];
					avgWordLength = Float.parseFloat(avgWordLengthStr);
				}
				else if(line.contains("Punctuation words density: ")) {
					String boundary = "Punctuation words density: ";
					String[] tokensVal = line.split(boundary);
					String punctDensityStr = tokensVal[1];
					punctuationDensity = Float.parseFloat(punctDensityStr);
				}
				else if(line.contains("Function words density: ")) {
					String boundary = "Function words density: ";
					String[] tokensVal = line.split(boundary);
					String funcDensityStr = tokensVal[1];
					functionDensity = Float.parseFloat(funcDensityStr);
				}
				else if(line.contains("@")) { //wordCount
					line = line.replace("@", "");
					String[] tokensVal = line.split("=");
					wordCount.put(tokensVal[0], new Integer(tokensVal[1]));
				}
				else if(line.contains("%")) { //couple
					line = line.replace("%", "");
					String[] tokensVal = line.split("=");
					String[] words = tokensVal[0].split("\\|");
					twoGrams.put(new TextPair(words[0], words[1]), new Integer(tokensVal[1]));
				}
				else if(line.contains("#")) { //trigrams
					line = line.replace("#", "");
					String[] tokensVal = line.split("=");
					String[] words = tokensVal[0].split("\\|");
					threeGrams.put(new TextTrigram(words[0], words[1], words[2]), new Integer(tokensVal[1]));
				}
			}
			
		}//end map
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			
		    InputSplit split = context.getInputSplit();
		    Class<? extends InputSplit> splitClass = split.getClass();

		    FileSplit fileSplit = null;
		    if (splitClass.equals(FileSplit.class)) {
		        fileSplit = (FileSplit) split;
		    } else if (splitClass.getName().equals(
		            "org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
		        // begin reflection hackery...

		        try {
		            Method getInputSplitMethod = splitClass
		                    .getDeclaredMethod("getInputSplit");
		            getInputSplitMethod.setAccessible(true);
		            fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
		        } catch (Exception e) {
		            // wrap and re-throw error
		            throw new IOException(e);
		        }

		        // end reflection hackery
		    }
			
			String filename = fileSplit.getPath().getName();
			
			boolean isKnown;
			
			if(filename.contains(",___,")) {
				isKnown = false;
			}
			else {
				isKnown = true;
			}
			
			if(isKnown) { //file1, known

				authorTrace.setAuthor(new Text(author));
				
				WordsArrayWritable wordCountWritable = new WordsArrayWritable();
				wordCountWritable.setArray(wordCount);
				authorTrace.setWordsArray(wordCountWritable);
				
				TwoGramsWritable twoGramsWritable = new TwoGramsWritable();
				twoGramsWritable.setTwoGrams(twoGrams);
				authorTrace.setFinalTwoGrams(twoGramsWritable);
				
				ThreeGramsWritable threeGramsWritable = new ThreeGramsWritable();
				threeGramsWritable.setThreeGrams(threeGrams);
				authorTrace.setFinalThreeGrams(threeGramsWritable);
				
				authorTrace.setFunctionDensity(new FloatWritable(functionDensity));
				authorTrace.setPunctuationDensity(new FloatWritable(punctuationDensity));
				authorTrace.setAvgWordLength(new FloatWritable(avgWordLength));
								
				context.write(authorTrace, new IntWritable(1));
				
			}
			else { //file0, unknown
				
				authorTrace.setAuthor(new Text("UNKNOWN"));
				
				WordsArrayWritable wordCountWritable = new WordsArrayWritable();
				wordCountWritable.setArray(wordCount);
				authorTrace.setWordsArray(wordCountWritable);
				
				TwoGramsWritable twoGramsWritable = new TwoGramsWritable();
				twoGramsWritable.setTwoGrams(twoGrams);
				authorTrace.setFinalTwoGrams(twoGramsWritable);
				
				ThreeGramsWritable threeGramsWritable = new ThreeGramsWritable();
				threeGramsWritable.setThreeGrams(threeGrams);
				authorTrace.setFinalThreeGrams(threeGramsWritable);
				
				authorTrace.setFunctionDensity(new FloatWritable(functionDensity));
				authorTrace.setPunctuationDensity(new FloatWritable(punctuationDensity));
				authorTrace.setAvgWordLength(new FloatWritable(avgWordLength));
								
				context.write(authorTrace, new IntWritable(0));
				
			}			
		}

	} //end Map class
	
	/*
	public static class authorFieldPartitioner extends Partitioner<AuthorTrace, IntWritable> {
		
		@Override
		public int getPartition(AuthorTrace key, IntWritable value, int numPartitions) {
			
			return (key.getAuthor().hashCode() * 29 & Integer.MAX_VALUE) % numPartitions;
			
		}
	}
	*/
	
	public static class Reduce extends Reducer<AuthorTrace, IntWritable, NullWritable, Text> {
		
		@Override
		public void reduce(AuthorTrace key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
		
			String toEmit = null;
			for (IntWritable value : values) {
				toEmit = key.toString();
				toEmit += "\n " + value;
				context.write(NullWritable.get(), new Text(toEmit));
			}
			
			/*
			String toEmit = null;
			for (IntWritable value : values) {
				if(value.get() == 1) {
					toEmit = key.toString();
					toEmit += "\n " + value;
					context.write(NullWritable.get(), new Text(toEmit));
				}
				else {
					toEmit = key.toString();
					toEmit += "\n " + value;
					context.write(NullWritable.get(), new Text(toEmit));
				}
			}
			*/
			
		}//end reduce
		
	} //end Reduce class

}//end AuthorAttributionSearch class
