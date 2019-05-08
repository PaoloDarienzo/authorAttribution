package creation;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

import support.AuthorTrace;
import support.BookTrace;
import support.MethodsCollection;
import support.TextPair;
import support.TextTrigram;
import support.ThreeGramsWritable;
import support.TwoGramsWritable;
import support.WordsArrayWritable;
import support.WordsFreqWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;

public class AuthorAttributionCreation extends Configured implements Tool {

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
		/*
		 * 
		 * MAP
		 * (K, V)
		 * KEY: AUTHOR
		 * VALUE: VALUES SET
		 * -> array of words (wordCount)
		 * -> n-grams
		 * REDUCE (K, receive per key/author)
		 * (K, V)
		 * needs to compute sum V
		 * -> sum of words (wordCount)
		 * -> stats on density values...
		 * -> n-grams
		 * 
		 * Key of reducer not needed to be written
		 */
		
		/*
		 * MAP INPUT: TEXTS
		 * MAP OUTPUT: BOOKTRACE
		 * ---partitioner---
		 * REDUCE INPUT: BOOKTRACE (of same author)
		 * REDUCE OUTPUT(S): AUTHORTRACE (of that author)
		 */
		
		int res = ToolRunner.run(new AuthorAttributionCreation(), args);
		System.exit(res);
		
	} //end main class
		
	public int run(String[] args) throws Exception {
					
		Job job = Job.getInstance(getConf(), "Profiles creation");
		job.setJarByClass(this.getClass());
		
		//For reading directories in input path recursively
		//not needed/not working
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
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BookTrace.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(AuthorTrace.class);
		
		job.setPartitionerClass(authorPartitioner.class);
		//job.setGroupingComparatorClass(GroupComparator.class);
		//job.setSortComparatorClass(Text.Comparator.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
		
	}
		
	public static class authorPartitioner extends Partitioner<Text, BookTrace> {
		
		@Override
		public int getPartition(Text key, BookTrace value, int numPartitions) {
			
			return (key.hashCode() * 163 & Integer.MAX_VALUE) % numPartitions;
			
		}
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, BookTrace> {
		
		private final static Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		private final static Pattern WORD_BOUNDARY_FOR_PUNCT = Pattern.compile("\\s*\\s*");
		private final static String AUTHOR_DELIMITER = ",___,";
		//. , : ; ? ! ( ) - "
		public static final String[] SET_PUNCT_VALUES = new String[] {	".", ",", ":", ";",
																		"?", "!", "(", ")",
																		"-", "\""};
		public static final Set<String> PUNCTUATION = new HashSet<>(Arrays.asList(SET_PUNCT_VALUES));
		
		private Text author = new Text();
		
		private BookTrace currentBookTrace;
		private boolean isWord;
		private int punctNo, funcNo;
		private String post, postPost;
				
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			currentBookTrace = new BookTrace();
			isWord = false;
			punctNo = 0;
			funcNo = 0;
			post = ""; postPost = "";
		}
		
		@Override
		public void map(LongWritable offset, Text lineText, Context context) 
				throws IOException, InterruptedException {
						
			String line = lineText.toString();
			
			//twoGrams/threeGrams
			List<String> words = new ArrayList<String>();
			
			for(String word : WORD_BOUNDARY.split(line)) {
				//now the line is splitted in words;
				//punctuation symbols that are in block, e. g. "!?", are not divided yet.
				
				if (word.isEmpty()) {
					//skip if word is empty
					continue;
				}
				else if ((!(word.matches("^[a-zA-Z0-9]*$")) && !(PUNCTUATION.contains(word)))) {
					//will skip if word is not a digit or a char
					//and is not a punctuation symbol
					//NB: set of punctuation are not checked yet
					
					//Dividing block of punctuation symbols and analyzing
					for(String wordSymb : WORD_BOUNDARY_FOR_PUNCT.split(word)) {
						//now the word is splitted in singular punctuation symbols;
						//only punctuation will be accepted, else continue.
						if (wordSymb.isEmpty()
								||
								!(PUNCTUATION.contains(wordSymb))
							){
							continue; //skipping to next block to analyze
						}
						else {
							//always true, but better check
							if(MethodsCollection.punctuationChecker(wordSymb)) {
								punctNo++;
							}
							//wordCount
							currentBookTrace.addWord(wordSymb);
						}
					  } //end cycle over block of punctuation symbols
					
				}	//end if over not words or not numbers or not singular punctuation symbols
					//or block of singular punctuation symbols
				
				else {
					//only words with digit or chars get here, or single punctuation symbols;
					//not other symbols, nor punctuation blocks
					isWord = true;
					if(MethodsCollection.punctuationChecker(word)) {
						punctNo++;
						isWord = false;
					}
					else if(MethodsCollection.functionWordChecker(word)) {
						funcNo++;
					}
					String lowCaseWord = word.toLowerCase();
					//wordCount
					currentBookTrace.addWord(lowCaseWord);
					if(isWord) {
						//twoGrams/threeGrams with only words
						words.add(lowCaseWord);
					}
				}
			} //end for
			
			//twoGrams and threeGrams creating and counting
			//(n-grams over a single line)
			int indexTerm = -1;
			for(String term : words) {
				post = ""; postPost = "";
				indexTerm++;
				if(!(indexTerm + 2 >= words.size())) {
					//if postPost exists, post exists
					postPost = words.get(indexTerm + 2);
					post = words.get(indexTerm + 1);
				}
				else if(!(indexTerm + 1 >= words.size())) {
					post = words.get(indexTerm + 1);
				}
				if(!postPost.isEmpty()) {
					//if postPost exists, post exists
					currentBookTrace.addTrigram(new TextTrigram(term, post, postPost));
					//currentBookTrace.commenti += "\n" + new TextTrigram(term, post, postPost).toString();
					currentBookTrace.addPair(new TextPair(term, post));
					//currentBookTrace.commenti += "\n" + post + ", " + term + "\n";
				}
				else if(!post.isEmpty()) {
					currentBookTrace.addPair(new TextPair(term, post));
					//currentBookTrace.commenti += "\n" + post + ", " + term + "\n";
				}
				//fixed word w, emits a (new) pair <w, w+1> (if w1+1 exists)
				//and a (new) trigram <w, w+1, w+2> (if w+2 exists)
				//I cannot use a single pair and then changing
				//through the set method the two words, because the
				//map that stores all the pairs, are referenced
				//to the SAME pair; in that case, we will have all the pair
				//with the same string in first and second field.
				//So the string field's a pass by reference and not pass by value
			}//end twoGrams and threeGrams for cycle
			
		}//end map
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			
			//Extracting author name from filename
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			String[] tokensVal = filename.split(AUTHOR_DELIMITER);
			author.set(tokensVal[0]);
			
			//wordCount support phase
			currentBookTrace.setPunctNo(new IntWritable(punctNo));
			currentBookTrace.setFuncNo(new IntWritable(funcNo));
			
			context.write(author, currentBookTrace);
		}

	} //end Mapper class

	/*
	 * Each reducer gets all jobs relatively to an author, thanks to the partitioner
	 */
	public static class Reduce extends Reducer<Text, BookTrace, NullWritable, AuthorTrace> {

		private MultipleOutputs<NullWritable, AuthorTrace> multipleOutputs;
		
		private AuthorTrace authTrace;
		
		private int nBooks;
		private int totalPunct, totalFunc;
		private long totalChars, numWords;
		private float TTR;
		
		private WordsArrayWritable HFinal; //for counting words
		private TwoGramsWritable finalTwoGrams; //for twoGrams
		private ThreeGramsWritable finalThreeGrams; //for threeGrams
		private FloatWritable avgWordLength, punctuationDensity, functionDensity;
				
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			
			multipleOutputs = new MultipleOutputs<NullWritable, AuthorTrace>(context);
			
		}
		
		@Override
		public void reduce(Text key, Iterable<BookTrace> values, Context context) 
				throws IOException, InterruptedException {
			
			authTrace = new AuthorTrace();
			
			nBooks = 0;
			totalPunct = 0;
			totalFunc = 0;
			totalChars = 0;
			numWords = 0;
			TTR = 0;
			
			HFinal = new WordsArrayWritable(); //for counting words
			finalTwoGrams = new TwoGramsWritable();
			finalThreeGrams = new ThreeGramsWritable();
			avgWordLength = new FloatWritable(0);
			punctuationDensity = new FloatWritable(0);
			functionDensity = new FloatWritable(0);
			
			authTrace.setAuthor(key);
		    
			for (BookTrace book : values) {
				
				nBooks++;
				
				//summing array of words of each analyzed book
				HFinal.sum(book.getWordsArray());
				//Having a final array HFinal with total numbers of utilized words of each author
				
				//summing twoGrams from each book
				finalTwoGrams.sum(book.getTwoGramsWritable());
				
				//summing threeGrams from each book
				finalThreeGrams.sum(book.getThreeGramsWritable());
								
				//summing punctuation words
				totalPunct += book.getPunctNo().get();
				//summing function words
				totalFunc += book.getFuncNo().get();
				
				//Summing characters of every words contained in the book (passed as value)
				//the method excludes punctuation words
				totalChars += MethodsCollection.getTotalCharsOnWords(book.getWordsArray().getArray());
				
				//Taking total number of words
				//i.e. single words multiplied for the times they appear
				numWords += MethodsCollection.getTotalWords(book.getWordsArray().getArray());
				
				//authTrace.commenti += book.commenti;
			}
			
			//Setting the number of books used for generate that author profile
			authTrace.setNBooks(new IntWritable(nBooks));
					    
			//Ordering HashMap by key; used
		    //authTrace.setTreeWordsArray(orderArray(HFinal.getArray()));
			//HFinal has the count of each word used by that author in every book analyzed
			//numWords contains the number of total words used
			HashMap<String, Float> wordFreq = MethodsCollection.getWordFrequencies(HFinal.getArray(), numWords);
			//wordFreq;
			//finalTwoGrams;
			//finalThreeGrams;
			authTrace.setWordsFreqArray(
					new WordsFreqWritable(
							MethodsCollection.extractSubSetOrdered(wordFreq, 20, 60)
							)
					);
			
		    //Setting twoGram of author
		    authTrace.setTwoGramsKey(
		    		new TwoGramsWritable(
		    				MethodsCollection.extractSubSetOrderedTextPair(
		    						finalTwoGrams.getTwoGrams(), 20, 60
		    						)
		    				)
		    		);
		    
		    //Setting threeGram of author
		    authTrace.setThreeGramsKey(
		    		new ThreeGramsWritable(
		    				MethodsCollection.extractSubSetOrderedTrigram(
				    				finalThreeGrams.getThreeGrams(), 20, 60
				    				)
		    				)
		    		);
		    
		    //Excluding punctuation when calculating average word length
		    float avgWordLenFloat = (float) ((double) totalChars / (double) (numWords - totalPunct));
		    avgWordLength = new FloatWritable(avgWordLenFloat);
		    authTrace.setAvgWordLength(avgWordLength);
		    
		    punctuationDensity = new FloatWritable((float) totalPunct / (float) numWords);
		    authTrace.setPunctuationDensity(punctuationDensity);
		    
		    functionDensity = new FloatWritable((float) totalFunc / (float) numWords);
		    authTrace.setFunctionDensity(functionDensity);
		    
		    TTR = MethodsCollection.getTTR(HFinal.getArray());
		    authTrace.setTTR(new FloatWritable(TTR));
		    
		    //context.write(key, TraceFinal);
			multipleOutputs.write(NullWritable.get(), authTrace, key.toString());
			
		}//end reduce
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}	
		
	} //end Reducer class	
	
} //end AuthorAttributionCreation class