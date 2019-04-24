package test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import support.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;

public class AuthorAttrDistrCache extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new AuthorAttrDistrCache(), args);
		System.exit(res);
		
	} //end main class
	
	public int run(String[] args) throws Exception {
		
		//Path inPath = new Path(args[0]);
		Path outPath = new Path(args[1]);
		Path creationPath = new Path(outPath, "creation");
		Path resultPath = new Path(outPath, "result");
		Path toMatchPath = new Path("/user/paolo/authorAttr/output/creation/veryshort");
		
		Job search = Job.getInstance(getConf(), "Using distributed cache");
		search.setJarByClass(this.getClass());
		
		////////////
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
		//DistributedCache.addCacheFile(creationPath.toUri(), search.getConfiguration());
		
		FileInputFormat.addInputPath(search, toMatchPath);
		FileOutputFormat.setOutputPath(search, resultPath);
		
		search.setNumReduceTasks(Integer.parseInt(args[2]));
		search.setNumReduceTasks(0);
		
		search.setMapperClass(Map.class);
		
		search.setMapOutputKeyClass(Text.class);
		search.setMapOutputValueClass(IntWritable.class);
		search.setOutputKeyClass(NullWritable.class);
		search.setOutputValueClass(Text.class);
				
		return search.waitForCompletion(true) ? 0 : 1;
				
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private static AuthorTrace authorTraceUnk;
		private static AuthorTrace authorTrace;
		
		//AuthorTrace fields
		private static String author;
		private static float avgWordLength;
		private static float punctuationDensity;
		private static float functionDensity;
		private static HashMap<String, Integer> wordCount;
		private static HashMap<TextPair, Integer> twoGrams;
		private static HashMap<TextTrigram, Integer> threeGrams;
		
		private static String commenti;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
		
			commenti = "";
			
			authorTraceUnk = new AuthorTrace();
			authorTrace = new AuthorTrace();
			
			author = "";
			avgWordLength =	punctuationDensity = functionDensity = 0;
			wordCount = new HashMap<>();
			twoGrams = new HashMap<>();
			threeGrams = new HashMap<>();
			
			try{
	            Path[] unkFileProfiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
	           	            
	            if(unkFileProfiles != null && unkFileProfiles.length > 0) {
					//if only 1 unk file is passed,
					//the for cycle can be eliminated
					//readFile(unkFileProfiles[0]);
					 for(Path unkFileProfile : unkFileProfiles) {
					 	readFile(unkFileProfile);
					 	}
					 commenti += "at the end of reading, auth unk: " + authorTraceUnk.getAuthor().toString() + "\n";
	            }
	        } catch(IOException ex) {
	            System.err.println("Exception in mapper setup: " + ex.getMessage());
	        }
			
		}
		
		private void readFile(Path filePath) {
			
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
	            	
	            } //end while (cycling over lines in file)
	            
	            authorTraceUnk.setAuthor(new Text("UNKNOWN"));
				
				WordsArrayWritable wordCountWritable = new WordsArrayWritable();
				wordCountWritable.setArray(wordCount);
				authorTraceUnk.setWordsArray(wordCountWritable);
				
				TwoGramsWritable twoGramsWritable = new TwoGramsWritable();
				twoGramsWritable.setTwoGrams(twoGrams);
				authorTraceUnk.setFinalTwoGrams(twoGramsWritable);
				
				ThreeGramsWritable threeGramsWritable = new ThreeGramsWritable();
				threeGramsWritable.setThreeGrams(threeGrams);
				authorTraceUnk.setFinalThreeGrams(threeGramsWritable);
				
				authorTraceUnk.setFunctionDensity(new FloatWritable(functionDensity));
				authorTraceUnk.setPunctuationDensity(new FloatWritable(punctuationDensity));
				authorTraceUnk.setAvgWordLength(new FloatWritable(avgWordLength));
	            
				commenti += "unk auth at the end set: " + authorTraceUnk.getAuthor().toString() + "\n";
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
			
			
		}//end map
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
						
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
			
			String total = "";
			total += commenti + "\n\nEnd of map phase: \n";
			total += authorTrace.getAuthor().toString() + "\n############\n" + authorTraceUnk.toString() + "\n\n\n";
			context.write(new Text(total), new IntWritable(1));
			
		}

	} //end Map class

}//end AuthorAttributionSearch class
