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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;

//TODO
/*
 * line length -> number of sentences (". ", "?", "!")
 * 
 * Sistemare stampa dei commenti;
 * Sistemare conteggio frasi;
 * Aggiungere concetto vicinanza;
 * 
 */

/*
 * The default text separator is the newline char (\n);
 * Everytime the program encounters a stop line (?, !, .)
 * increments the sentence counter.
 * For counting the ".", we use as escape dot and space, ". ".
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
		private int lineNo, puntNo, funcNo;
				
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			currentBookTrace = new BookTrace();
			lineNo = 0;
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
			lineNo++;
			
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
					if(MethodsCollection.puntuactionChecker(word)) {
						puntNo++;
					}
					else if(MethodsCollection.functionWordChecker(word)) {
						funcNo++;
					}
				}
				
				//wordCount
				currentBookTrace.addWord(word.toLowerCase());
								
			} //end for
			
			//wordCount support phase
			currentBookTrace.setLineNo(new IntWritable(lineNo));
			currentBookTrace.setpuntNo(new IntWritable(puntNo));
			currentBookTrace.setfuncNo(new IntWritable(funcNo));
			
			/////
			currentBookTrace.commenti =	"\nBook: " + filename +
										"\nlineNo " + lineNo +
										"\npuntNo " + puntNo +
										"\nfuncNo " + funcNo + "\n";
			
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
		
		private int nBooks, totalLines, totalPunt, totalFunc;
		private long totalChars, numWords;
		
		private ArrayWritable HFinal; //for counting words
		private FloatWritable avgNoLine, avgWordLength, puntuactionDensity, functionDensity;
				
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			
			multipleOutputs = new MultipleOutputs<NullWritable, AuthorTrace>(context);
			
			authTrace = new AuthorTrace();
			
			nBooks = 0;
			totalLines = 0;
			totalPunt = 0;
			totalFunc = 0;
			totalChars = 0;
			numWords = 0;
			
			HFinal = new ArrayWritable(); //for counting words
			avgNoLine = new FloatWritable(0);
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
				
				//sommo tutti gli array di words
				//dei vari libri analizzati
				HFinal.sum(book.getArray());
				//avrò un array finale con il numero totale
				//di parole utilizzate da un determinato autore
				
				//sommo le linee/frasi totali
				totalLines += book.getLineNo().get();
				
				//sommo punctuaction words
				totalPunt += book.getpuntNo().get();
				//sommo function words
				totalFunc += book.getfuncNo().get();
				
				//somma i caratteri di tutte le parole contenute nel
				//libro passato come valore;
				//ogni libro analizzato è un value
				totalChars += MethodsCollection.getTotalChars(book.getArray().getArray());
				
				//prendo il numero di parole totali,
				//ovvero le singole parole moltiplicate per le volte in cui compaiono
				numWords += MethodsCollection.getTotalWords(book.getArray().getArray());
				
				authTrace.commenti += book.commenti;
			}
					    
			//Ordering HashMap by key:
		    //TreeMap to store values of HashMap 
		    TreeMap<String, Integer> finalWordValOrdered = new TreeMap<>();
		    // Copy all data from hashMap into TreeMap 
		    finalWordValOrdered.putAll(HFinal.getArray());
		    authTrace.setTreeWordsArray(finalWordValOrdered);
		    
		    avgNoLine = new FloatWritable((float) totalLines / (float) nBooks);
		    authTrace.setAvgNoLine(avgNoLine);
		    
		    //Excluding puntuaction when calculating average word length
		    float avgWordLenFloat = (float) ((double) totalChars / (double) (numWords - totalPunt));
		    avgWordLength = new FloatWritable(avgWordLenFloat);
		    authTrace.setAvgWordLength(avgWordLength);
		    
		    puntuactionDensity = new FloatWritable((float) totalPunt / (float) numWords);
		    authTrace.setPuntuactionDensity(puntuactionDensity);
		    
		    functionDensity = new FloatWritable((float) totalFunc / (float) numWords);
		    authTrace.setFunctionDensity(functionDensity);
		    
		    authTrace.commenti +=	"\nAutore: " + key.toString() + 
		    						"\n N books: " + nBooks +
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