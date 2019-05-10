package search;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import support.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;

/**
 * 
 * @author Paolo D'Arienzo
 *
 */
public class AuthorAttributionSearch extends Configured implements Tool{
	
	/**
	 * 
	 * @param args args[0] input path, args[1] output path, args[2] number of reducers, args[3] profiles repository path
	 * @throws Exception if run tool throws error
	 */
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new AuthorAttributionSearch(), args);
		System.exit(res);
		
	} //end main class
	
	public int run(String[] args) throws Exception {
		
		//arg0: path to input file to analyze
		//arg1: path to output directory
		//arg2: number of reducers (same as number of authors/unk files)
		//arg3: path to author-known files to compare with
		
		Path inPath = new Path(args[0]);
		Path outPath = new Path(args[1]);
		Path creationPath = new Path(outPath, "creation");
		Path resultPath = new Path(outPath, "result");
		//Path toMatchPath = new Path("/user/paolo/authorAttr/output/creation/veryshort");
		//Path toMatchPath = new Path("/user/paolo/authorAttr/output/testShort/creation");
		Path toMatchPath = new Path(args[3]);
		
		int numReducer = Integer.parseInt(args[2]);
		
		//JOB 1: Creation profile of unknown file
		///////////////////////////////////////////////////////////////////////////////////////////

		Job creation = Job.getInstance(getConf(), "1. Creation profile(s) of unknown file(s)");
		creation.setJarByClass(this.getClass());
		
		FileInputFormat.addInputPath(creation, inPath);
		FileOutputFormat.setOutputPath(creation, creationPath);
		LazyOutputFormat.setOutputFormatClass(creation, TextOutputFormat.class);
		
		//CRITICAL
		creation.setNumReduceTasks(numReducer);
		
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
		
		/////////////////////////////////////////
		//Loading output of first job in distributed cache
		FileSystem fs = FileSystem.get(getConf());
	    
	    //the unknown files don't have the author name in the file name,
	    //so they don't have the separator neither; thus, they are renamed
	    //with the extended file name, extension .txt included
	    FileStatus[] fileList = fs.listStatus(creationPath, 
	                               new PathFilter(){
	                                     @Override public boolean accept(Path path){
	                                    	 	return path.getName().contains(".txt");
	                                     } 
	                                } );
		
		for(int i=0; i < fileList.length; i++){ 
            DistributedCache.addCacheFile(fileList[i].getPath().toUri(), search.getConfiguration());
		}
		/////////////////////////////////////////
		
		FileInputFormat.addInputPath(search, toMatchPath);
		FileOutputFormat.setOutputPath(search, resultPath);
		
		LazyOutputFormat.setOutputFormatClass(search, TextOutputFormat.class);
		
		//CRITICAL
		search.setNumReduceTasks(numReducer);
		
		search.setMapperClass(Map.class);
		search.setReducerClass(Reduce.class);
		
		search.setPartitionerClass(search.AuthorAttributionSearch.authorPartitioner.class);
		
		search.setMapOutputKeyClass(AuthorTrace.class);
		search.setMapOutputValueClass(StatsWritable.class);
		search.setOutputKeyClass(NullWritable.class);
		search.setOutputValueClass(Text.class);
				
		return search.waitForCompletion(true) ? 0 : 1;
				
	}
	
	public static class Map extends Mapper<LongWritable, Text, AuthorTrace, StatsWritable> {
		
		private static AuthorTrace authorTrace;
		//for multiple unknown file at once, implement
		private static ArrayList<AuthorTrace> authorsUnk;
		//private static AuthorTrace authorTraceUnk;
		
		//AuthorTrace fields
		private static String author;
		private static float avgWordLength;
		private static float punctuationDensity;
		private static float functionDensity;
		private static float TTR;
		private static HashMap<String, Float> wordFreq;
		private static HashMap<TextPair, Integer> twoGrams;
		private static HashMap<TextTrigram, Integer> threeGrams;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
		
			authorTrace = new AuthorTrace();
			//for multiple unknown file at once, implement
			authorsUnk = new ArrayList<>();
			//authorTraceUnk = new AuthorTrace();
			
			author = "";
			avgWordLength =	punctuationDensity = functionDensity = TTR = 0;
			wordFreq = new HashMap<>();
			twoGrams = new HashMap<>();
			threeGrams = new HashMap<>();
			
			//Reading from distributed cache
			try{
	            Path[] unkFileProfiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());       	            
	            if(unkFileProfiles != null && unkFileProfiles.length > 0) {
					//if only 1 unk file is passed,
					//the for cycle can be eliminated
					//readFile(unkFileProfiles[0], context);
					 for(Path unkFileProfile : unkFileProfiles) {
					 	readFile(unkFileProfile, context);
					 	}
				}
	        } catch(IOException ex) {
	            System.err.println("Exception in mapper setup: " + ex.getMessage());
	        }
			
		} //end setup
		
		//Reading files from distributed cache
		/**
		 * Reads file from distributed cache; must not exceed 10GB
		 * @param filePath path where are stored profile of unknown authors
		 * @param context context
		 */
		private void readFile(Path filePath, Context context) {
			
			AuthorTrace authorTraceUnk = new AuthorTrace();
			String filename = "";
			
			//AuthorTrace fields
			float avgWordLength, punctuationDensity, functionDensity, TTR;
			avgWordLength = punctuationDensity = functionDensity = TTR = 0;
			HashMap<String, Float> wordFreq = new HashMap<String, Float>();
			HashMap<TextPair, Integer> twoGrams = new HashMap<TextPair, Integer>();
			HashMap<TextTrigram, Integer> threeGrams = new HashMap<TextTrigram, Integer>();
			
	        try{
	            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
	            String line = null;
	            	            
	            while((line = bufferedReader.readLine()) != null) {
	            	
	            	//CRITICAL
	            	if(line.contains("Author profile: ")) {
	    				String boundary = "Author profile: ";
	    				String[] tokensVal = line.split(boundary);
	    				filename = tokensVal[1];
	    			}
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
	    			else if(line.contains("TTR: ")) {
	    				String boundary = "TTR: ";
	    				String[] tokensVal = line.split(boundary);
	    				String TTRStr = tokensVal[1];
	    				TTR = Float.parseFloat(TTRStr);
	    			}
	    			else if(line.contains("@")) { //wordCount
	    				line = line.replace("@", "");
	    				String[] tokensVal = line.split("=");
	    				String wordFreqStr = tokensVal[1];
	    				wordFreq.put(tokensVal[0], Float.parseFloat(wordFreqStr));
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
	            	
	            } //end while (cycling over lines in file)
	            
	            //CRITICAL
	            authorTraceUnk.setAuthor(new Text(filename));
	            
	            authorTraceUnk.setAvgWordLength(new FloatWritable(avgWordLength));
	            
	            authorTraceUnk.setFunctionDensity(new FloatWritable(functionDensity));
				authorTraceUnk.setPunctuationDensity(new FloatWritable(punctuationDensity));
				authorTraceUnk.setTTR(new FloatWritable(TTR));
				
				WordsFreqWritable wordFreqWritable = new WordsFreqWritable();
				wordFreqWritable.setArray(wordFreq);
				authorTraceUnk.setWordsFreqArray(wordFreqWritable);
				
				TwoGramsWritable twoGramsWritable = new TwoGramsWritable();
				twoGramsWritable.setTwoGrams(twoGrams);
				authorTraceUnk.setTwoGramsKey(twoGramsWritable);
				
				ThreeGramsWritable threeGramsWritable = new ThreeGramsWritable();
				threeGramsWritable.setThreeGrams(threeGrams);
				authorTraceUnk.setThreeGramsKey(threeGramsWritable);
	           
				authorsUnk.add(authorTraceUnk);
				bufferedReader.close();
				
	        } catch(IOException ex) {
	            System.err.println("Exception while reading file: " + ex.getMessage());
	        }
	        
	    } //end readFile method
		
		@Override
		public void map(LongWritable offset, Text lineText, Context context) 
				throws IOException, InterruptedException {
			
			//offset is all the file; the default separator is \n;
			//lineText is a line of that file.
			String line = lineText.toString();
			
			if(line.contains("Author profile: ")) {
				String boundary = "Author profile: ";
				String[] tokensVal = line.split(boundary);
				author = tokensVal[1];
			}
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
			else if(line.contains("TTR: ")) {
				String boundary = "TTR: ";
				String[] tokensVal = line.split(boundary);
				String TTRStr = tokensVal[1];
				TTR = Float.parseFloat(TTRStr);
			}
			else if(line.contains("@")) { //wordCount
				line = line.replace("@", "");
				String[] tokensVal = line.split("=");
				String wordFreqStr = tokensVal[1];
				wordFreq.put(tokensVal[0], Float.parseFloat(wordFreqStr));
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
			
		}//end map
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			
			authorTrace.setAuthor(new Text(author));
			
			authorTrace.setAvgWordLength(new FloatWritable(avgWordLength));
			
			authorTrace.setFunctionDensity(new FloatWritable(functionDensity));
			authorTrace.setPunctuationDensity(new FloatWritable(punctuationDensity));
			authorTrace.setTTR(new FloatWritable(TTR));
			
			WordsFreqWritable wordFreqWritable = new WordsFreqWritable();
			wordFreqWritable.setArray(wordFreq);
			authorTrace.setWordsFreqArray(wordFreqWritable);
			
			TwoGramsWritable twoGramsWritable = new TwoGramsWritable();
			twoGramsWritable.setTwoGrams(twoGrams);
			authorTrace.setTwoGramsKey(twoGramsWritable);
			
			ThreeGramsWritable threeGramsWritable = new ThreeGramsWritable();
			threeGramsWritable.setThreeGrams(threeGrams);
			authorTrace.setThreeGramsKey(threeGramsWritable);
			
			
			//////////////////////////////////////////////////
			//now authorTrace and authorTraceUnk are set
			//StatsWritable stats = new StatsWritable(authorTrace, authorTraceUnk);
			//context.write(authorTraceUnk, stats);
			
			for(AuthorTrace unk : authorsUnk) {
				StatsWritable stats = new StatsWritable(authorTrace, unk);
				context.write(unk, stats);
			}
			
		} //end cleanup

	} //end Map class
	
	public static class authorPartitioner extends Partitioner<AuthorTrace, StatsWritable> {
		
		@Override
		public int getPartition(AuthorTrace key, StatsWritable value, int numPartitions) {
			
			return (key.getAuthor().hashCode() * 17 & Integer.MAX_VALUE) % numPartitions;
			
		}
	}
	
	public static class Reduce extends Reducer<AuthorTrace, StatsWritable, NullWritable, Text> {
		
		private MultipleOutputs<NullWritable, Text> multipleOutputs;
		
		//weight factors
		private static float avgWordLenFact = 1.5f;
		private static float funDensFact = 3;
		private static float punctDensFact = 2;
		private static float ttrFact = 1.5f;
		private static float wordFreqFact = 1;
		private static float twoGramsFact = 0.5f;
		private static float threeGramsFact = 0.5f;
		
		//for weighted sum
		private static float sum = avgWordLenFact + funDensFact + punctDensFact +
					ttrFact + wordFreqFact + twoGramsFact + threeGramsFact;
		private static float numFact = (float) sum;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		}
		
		@Override
		public void reduce(AuthorTrace unk, Iterable<StatsWritable> statsSet, Context context) 
				throws IOException, InterruptedException {
				
			//results is as big as number of authors known
			HashMap<String, Float> results = new HashMap<String, Float>(1024);
			
			String result = "START: " + unk.getAuthor() + "\n";
			
			String variousStats = "";
			
			for (StatsWritable stats : statsSet) {
				
				float overrallScore = 0;
				overrallScore += stats.getAvgWordLengthRatio().get() * avgWordLenFact;
				overrallScore += stats.getFunctionDensityRatio().get() * funDensFact;
				overrallScore += stats.getPunctuationDensityRatio().get() * punctDensFact;
				overrallScore += stats.getTypeTokenRatio().get() * ttrFact;
				overrallScore += stats.getWordFreqRatioRatio().get() * wordFreqFact;
				overrallScore += stats.getTwoGramsRatio().get() * twoGramsFact;
				overrallScore += stats.getThreeGramsRatio().get() * threeGramsFact;
				
				//<referring to author, similarity score>
				results.put(stats.getOnAuthor().toString(), new Float(overrallScore / numFact));
				
				variousStats += "\n" + stats.toString();
				
			}
			
		
			String resultsOrdered = MethodsCollection.orderHashMapByValueToString(results);
			
			result += resultsOrdered + "\n" + variousStats;
			//context.write(NullWritable.get(), new Text(result));
			multipleOutputs.write(NullWritable.get(), new Text(result), unk.getAuthor().toString());
			
		}//end reduce
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {	
			multipleOutputs.close();
		}
		
	} //end Reduce class

}//end AuthorAttributionSearch class
