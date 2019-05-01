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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import support.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;

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
		//Path toMatchPath = new Path("/user/paolo/authorAttr/output/creation/veryshort");
		Path toMatchPath = new Path("/user/paolo/authorAttr/output/testShort/creation");
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
		search.setNumReduceTasks(Integer.parseInt(args[2]));
		
		search.setMapperClass(Map.class);
		search.setReducerClass(Reduce.class);
		
		search.setMapOutputKeyClass(AuthorTrace.class);
		search.setMapOutputValueClass(StatsWritable.class);
		search.setOutputKeyClass(NullWritable.class);
		search.setOutputValueClass(Text.class);
				
		return search.waitForCompletion(true) ? 0 : 1;
				
	}
	
	public static class Map extends Mapper<LongWritable, Text, AuthorTrace, StatsWritable> {
		
		private static AuthorTrace authorTrace;
		//for multiple unknown file at once, implement
		//ArrayList<AuthorTrace> authorsUnk;
		private static AuthorTrace authorTraceUnk;
		
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
			//authorsUnk = new ArrayList<>();
			authorTraceUnk = new AuthorTrace();
			
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
					readFile(unkFileProfiles[0]);
	            	/*
					 for(Path unkFileProfile : unkFileProfiles) {
					 	readFile(unkFileProfile);
					 	}
					 */
				}
	        } catch(IOException ex) {
	            System.err.println("Exception in mapper setup: " + ex.getMessage());
	        }
			
		}
		
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
			authorTrace.setFinalTwoGrams(twoGramsWritable);
			
			ThreeGramsWritable threeGramsWritable = new ThreeGramsWritable();
			threeGramsWritable.setThreeGrams(threeGrams);
			authorTrace.setFinalThreeGrams(threeGramsWritable);
			
			
			//////////////////////////////////////////////////
			//now authorTrace and authorTraceUnk are set
			
			StatsWritable stats = new StatsWritable(authorTrace, authorTraceUnk);
			
			context.write(authorTraceUnk, stats);
			
		} //end cleanup

		private void readFile(Path filePath) {
			
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
	            	
	    			if(line.contains("Avg word length: ")) {
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
	            
	            authorTraceUnk.setAuthor(new Text("UNKNOWN"));
	            
	            authorTraceUnk.setAvgWordLength(new FloatWritable(avgWordLength));
	            
	            authorTraceUnk.setFunctionDensity(new FloatWritable(functionDensity));
				authorTraceUnk.setPunctuationDensity(new FloatWritable(punctuationDensity));
				authorTraceUnk.setTTR(new FloatWritable(TTR));
				
				WordsFreqWritable wordFreqWritable = new WordsFreqWritable();
				wordFreqWritable.setArray(wordFreq);
				authorTraceUnk.setWordsFreqArray(wordFreqWritable);
				
				TwoGramsWritable twoGramsWritable = new TwoGramsWritable();
				twoGramsWritable.setTwoGrams(twoGrams);
				authorTraceUnk.setFinalTwoGrams(twoGramsWritable);
				
				ThreeGramsWritable threeGramsWritable = new ThreeGramsWritable();
				threeGramsWritable.setThreeGrams(threeGrams);
				authorTraceUnk.setFinalThreeGrams(threeGramsWritable);
	           
				bufferedReader.close();
				
	        } catch(IOException ex) {
	            System.err.println("Exception while reading file: " + ex.getMessage());
	        }
	        
	    } //end readFile method

	} //end Map class
	
	public static class Reduce extends Reducer<AuthorTrace, StatsWritable, NullWritable, Text> {
		
		//weight factors
		private static float avgWordLenFact = 1;
		private static float funDensFact = 3;
		private static float punctDensFact = 2;
		private static float ttrFact = (float) 1.5;
		private static float wordFreqFact = 1;
		private static float twoGramsFact = 1;
		private static float threeGramsFact = 1;
		
		//for weighted sum
		private static float sum = avgWordLenFact + funDensFact + punctDensFact +
					ttrFact + wordFreqFact + twoGramsFact + threeGramsFact;
		private static float numFact = (float) sum;
		
		@Override
		public void reduce(AuthorTrace unk, Iterable<StatsWritable> statsSet, Context context) 
				throws IOException, InterruptedException {
				
			HashMap<String, Float> results = new HashMap<String, Float>();
			
			//TODO
			//unk has in author "UNKNOWN"; could be replaced with the name of the file
			String result = "START\n";
			
			String varieStats = "";
			
			for (StatsWritable stats : statsSet) {
				
				float overrallScore = stats.getAvgWordLengthRatio().get() * avgWordLenFact;
				overrallScore += stats.getFunctionDensityRatio().get() * funDensFact;
				overrallScore += stats.getPunctuationDensityRatio().get() * punctDensFact;
				overrallScore += stats.getTypeTokenRatio().get() * ttrFact;
				overrallScore += stats.getWordFreqRatioRatio().get() * wordFreqFact;
				overrallScore += stats.getTwoGramsRatio().get() * twoGramsFact;
				overrallScore += stats.getThreeGramsRatio().get() * threeGramsFact;
				
				//<referring to author, similarity score>
				results.put(stats.getOnAuthor().toString(), new Float(overrallScore / numFact));
				
				varieStats += "\n" + stats.toString();
				
			}
			
		
			String resultsOrdered = MethodsCollection.orderHashMapByValueToString(results);
			
			result += resultsOrdered + "\n" + varieStats;
			context.write(NullWritable.get(), new Text(result));
			
		}//end reduce
		
	} //end Reduce class

}//end AuthorAttributionSearch class
