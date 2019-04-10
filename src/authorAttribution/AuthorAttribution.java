package authorAttribution;

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

import support.MethodsCollection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;

//TODO
/*
 * line length -> number of sentences (". ", "?", "!")
 * 
 * Aggiungere concetto vicinanza;
 * 
 */

public class AuthorAttribution extends Configured implements Tool {

	/**
	 * @param args input path, output path, number of reducers
	 */
	public static void main(String[] args) throws Exception {
		
		/*
		 * 
		 * MAP
		 * (K, V)
		 * CHIAVE: AUTORE
		 * VALORE: INSIEME DI VALORI
		 * -> array di words (wordCount)
		 * -> numero linee
		 * -> vicinanza
		 * REDUCE (K, riceve per chiave/autore)
		 * (K, V)
		 * deve fare somma V
		 * -> somma di words (wordCount)
		 * -> stat su numero linee, densità varie, lunghezze varie...
		 * -> vicinanza
		 * 
		 * Chiave del reduce non utile da scrivere
		 */
		
		/*
		 * MAP INPUT: TEXTS
		 * MAP OUTPUT: BOOKTRACE
		 * ---partitioner---
		 * REDUCE INPUT: BOOKTRACE (of same author)
		 * REDUCE OUTPUT(S): AUTHORTRACE (of that author)
		 */
		
		int res = ToolRunner.run(new AuthorAttribution(), args);
		System.exit(res);
		
	} //end main class
		
	public int run(String[] args) throws Exception {
					
		Job job = Job.getInstance(getConf(), "Author Attribution");
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
		private final static String AUTHOR_DELIMITER = ",___,";
		//. , : ; ? ! ( ) - "
		public static final String[] SET_PUNT_VALUES = new String[] {	".", ",", ":", ";",
																		"?", "!", "(", ")",
																		"-", "\""};
		public static final Set<String> PUNTUACTION = new HashSet<>(Arrays.asList(SET_PUNT_VALUES));
		
		private Text author = new Text();
		
		private BookTrace currentBookTrace;
		private TextPair currentPair; //for computing twoGrams
		private boolean isWord;
		private int puntNo, funcNo;
				
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			currentBookTrace = new BookTrace();
			currentPair = new TextPair(); //for computing twoGrams
			puntNo = 0;
			funcNo = 0;
		}
		
		@Override
		public void map(LongWritable offset, Text lineText, Context context) 
				throws IOException, InterruptedException {
						
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			String[] tokensVal = filename.split(AUTHOR_DELIMITER);
			author.set(tokensVal[0]);
						
			String line = lineText.toString();
			
			//twoGrams
			List<String> words = new ArrayList<String>();
			
			for (String word : WORD_BOUNDARY.split(line)) {
				//skip is word is empty
				//or is if is not a digit or a char
				//and is not a conjuction symbol
				if (word.isEmpty() 
						|| (
							!(word.matches("^[a-zA-Z0-9]*$")) && !(PUNTUACTION.contains(word))
							) 
					) {
					continue;
				}
				else {
					isWord = true;
					if(MethodsCollection.puntuactionChecker(word)) {
						puntNo++;
						isWord = false;
					}
					else if(MethodsCollection.functionWordChecker(word)) {
						funcNo++;
						isWord = false;
					}
				}
				
				//wordCount
				currentBookTrace.addWord(word.toLowerCase());
				if(isWord) {
					//twoGrams with only words
					words.add(word);	
				}
								
			} //end for
			
			//twoGrams creating and counting
			int indexSingle = -1;
			int indexOther = -1;
			for(String singleWord : words) {
				indexSingle++; //index of singleWord
				for(String otherWord : words) {
					indexOther++; //index of otherWord
					if(indexSingle == indexOther) { //avoiding word itself
						continue;
					}
					//fixed first word w1, cycling over every other word wn;
					//for each occurrence of <w1, wn>, emits a pair
					currentPair.set(singleWord, otherWord);
					currentBookTrace.addPair(currentPair);
				}
				indexOther = -1;
			}
			
			//wordCount support phase
			currentBookTrace.setpuntNo(new IntWritable(puntNo));
			currentBookTrace.setfuncNo(new IntWritable(funcNo));
			
			/////commenti del libro analizzato
			String intestazione = "\nBook: " + filename;
			String puntAndFunc = 	"\npuntNo " + puntNo +
									"\nfuncNo " + funcNo + "\n";
			String fromWordCount = "\n N° of different words: " +
									currentBookTrace.getWordsArray().getArray().size();
			String fromTwoGrams = "\n N° of woGrams counted: " +
									currentBookTrace.getTwoGramsWritable().getTwoGrams().size();

			currentBookTrace.commenti =	intestazione +
										fromWordCount +
										fromTwoGrams +
										puntAndFunc;
			
		}//end map
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			context.write(author, currentBookTrace);
		}

	} //end Mapper class

	/*
	 * Ogni reducer riceve tutti i lavori su un autore
	 */
	public static class Reduce extends Reducer<Text, BookTrace, NullWritable, AuthorTrace>{

		private MultipleOutputs<NullWritable, AuthorTrace> multipleOutputs;
		
		private AuthorTrace authTrace;
		
		private int nBooks, totalPunt, totalFunc;
		private long totalChars, numWords;
		
		private WordsArrayWritable HFinal; //for counting words
		private TwoGramsWritable finalTwoGrams; //for twoGrams
		private FloatWritable avgWordLength, puntuactionDensity, functionDensity;
				
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			
			multipleOutputs = new MultipleOutputs<NullWritable, AuthorTrace>(context);
			
			authTrace = new AuthorTrace();
			
			nBooks = 0;
			totalPunt = 0;
			totalFunc = 0;
			totalChars = 0;
			numWords = 0;
			
			HFinal = new WordsArrayWritable(); //for counting words
			finalTwoGrams = new TwoGramsWritable();
			avgWordLength = new FloatWritable(0);
			puntuactionDensity = new FloatWritable(0);
			functionDensity = new FloatWritable(0);
			
		}
		
		@Override
		public void reduce(Text key, Iterable<BookTrace> values, Context context) 
				throws IOException, InterruptedException {
			
			authTrace.setAuthor(key);
		    
			for (BookTrace book : values) {
				
				nBooks++;
				
				//summing array of words of each analyzed book
				HFinal.sum(book.getWordsArray());
				//Having final array with total numbers of utilized words of each author
				
				//summing twoGrams from each book
				finalTwoGrams.sum(book.getTwoGramsWritable());
								
				//sommo punctuaction words
				totalPunt += book.getpuntNo().get();
				//sommo function words
				totalFunc += book.getfuncNo().get();
				
				//somma i caratteri di tutte le parole contenute nel
				//libro passato come valore;
				//ogni libro analizzato è un value
				totalChars += MethodsCollection.getTotalChars(book.getWordsArray().getArray());
				
				//prendo il numero di parole totali,
				//ovvero le singole parole moltiplicate per le volte in cui compaiono
				numWords += MethodsCollection.getTotalWords(book.getWordsArray().getArray());
				
				authTrace.commenti +=	book.commenti;
			}
					    
			//Ordering HashMap by key:
		    //TreeMap to store values of HashMap 
		    TreeMap<String, Integer> finalWordValOrdered = new TreeMap<>();
		    // Copy all data from hashMap into TreeMap 
		    finalWordValOrdered.putAll(HFinal.getArray());
		    authTrace.setTreeWordsArray(finalWordValOrdered);
		    
		    //Setting twoGram of author
		    authTrace.setFinalTwoGrams(finalTwoGrams);
		    
		    //Excluding puntuaction when calculating average word length
		    float avgWordLenFloat = (float) ((double) totalChars / (double) (numWords - totalPunt));
		    avgWordLength = new FloatWritable(avgWordLenFloat);
		    authTrace.setAvgWordLength(avgWordLength);
		    
		    puntuactionDensity = new FloatWritable((float) totalPunt / (float) numWords);
		    authTrace.setPuntuactionDensity(puntuactionDensity);
		    
		    functionDensity = new FloatWritable((float) totalFunc / (float) numWords);
		    authTrace.setFunctionDensity(functionDensity);
		    
		    authTrace.commenti +=	"\nAutore: " + key.toString() + 
		    						"\n N° books: " + nBooks +
		    						"\n N° parole: " + numWords +
		    						"\n N° totale caratteri: " + totalChars +
		    						"\n N° function words: " + totalFunc +
		    						"\n N° puntuaction words: " + totalPunt + "\n";
		    						
		    //context.write(key, TraceFinal);
			multipleOutputs.write(NullWritable.get(), authTrace, key.toString());
			
		}//end reduce
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}		
		
	} //end Reducer class	
	
} //end AuthorAttribution class