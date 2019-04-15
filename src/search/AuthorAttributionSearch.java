package search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import support.*;

public class AuthorAttributionSearch extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new AuthorAttributionSearch(), args);
		System.exit(res);
		
	} //end main class
	
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf(), "Author Attribution Search");
		job.setJarByClass(this.getClass());
		
		//input e output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//number of reducers
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		
		//Avoid creating empty files (lazy output)
		//LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BookTrace.class);
		/*
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(AuthorTrace.class);
		*/
		
		//job.setPartitionerClass(authorPartitioner.class);
		//job.setGroupingComparatorClass(GroupComparator.class);
		//job.setSortComparatorClass(Text.Comparator.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
		
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, BookTrace> {
		
		private final static Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		//. , : ; ? ! ( ) - "
		public static final String[] SET_PUNT_VALUES = new String[] {	".", ",", ":", ";",
																		"?", "!", "(", ")",
																		"-", "\""};
		public static final Set<String> PUNTUACTION = new HashSet<>(Arrays.asList(SET_PUNT_VALUES));
		
		private BookTrace currentBookTrace;
		private boolean isWord;
		private int punctNo, funcNo;
		private String prec, prePrec, post, postPost;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			currentBookTrace = new BookTrace();
			isWord = false;
			punctNo = 0;
			funcNo = 0;
			prec = ""; prePrec = "";
			post = ""; postPost = "";
		}
		
		@Override
		public void map(LongWritable offset, Text lineText, Context context) 
				throws IOException, InterruptedException {
					
			String line = lineText.toString();
			
			//twoGrams/threeGrams
			List<String> words = new ArrayList<String>();
			
			for (String word : WORD_BOUNDARY.split(line)) {
				//skip is word is empty
				//or if is not a digit or a char
				//and is not a conjuction symbol
				if (word.isEmpty() 
						|| (
							!(word.matches("^[a-zA-Z0-9]*$")) && !(PUNTUACTION.contains(word))
							) 
					) {
					continue; //skipping to next legitimate word
				}
				else {
					isWord = true;
					if(MethodsCollection.puntuactionChecker(word)) {
						punctNo++;
						isWord = false;
					}
					else if(MethodsCollection.functionWordChecker(word)) {
						funcNo++;
					}
				}
				String lowCaseWord = word.toLowerCase();
				//wordCount
				currentBookTrace.addWord(lowCaseWord);
				if(isWord) {
					//twoGrams/threeGrams with only words
					words.add(lowCaseWord);
				}
								
			} //end for
			
			//twoGrams and threeGrams creating and counting
			int indexTerm = -1;
			for(String term : words) {
				prec = ""; prePrec = "";
				post = ""; postPost = "";
				indexTerm++;
				if(!(indexTerm - 2 < 0)) {
					prePrec = words.get(indexTerm - 2);
				}
				if(!(indexTerm - 1 < 0)) {
					prec = words.get(indexTerm - 1);
				}
				if(!(indexTerm + 2 >= words.size())) {
					postPost = words.get(indexTerm + 2);
				}
				if(!(indexTerm + 1 >= words.size())) {
					post = words.get(indexTerm + 1);
				}
				//if prePrec is empty, term is second word in sentence
				//so prePrec doesn't exists
				if(!prePrec.isEmpty()) {
					//if prePrec exists, prec exists
					currentBookTrace.addTrigram(new TextTrigram(term, prePrec, prec));
					//currentBookTrace.commenti += "\n" + new TextTrigram(term, prePrec, prec).toString();
				}
				if(!prec.isEmpty()) {
					currentBookTrace.addPair(new TextPair(term, prec));
					//currentBookTrace.commenti += "\n" + prec + ", " + term + "\n";
				}
				if(!post.isEmpty()) {
					currentBookTrace.addPair(new TextPair(term, post));
					//currentBookTrace.commenti += "\n" + post + ", " + term + "\n";
				}
				if(!postPost.isEmpty()) {
					//if postPost exists, post exists
					currentBookTrace.addTrigram(new TextTrigram(term, post, postPost));
					//currentBookTrace.commenti += "\n" + new TextTrigram(term, post, postPost).toString();
				}
				//fixed first word w1, cycling over every other word wn;
				//for each occurrence of <w1, wn>, emits a (new) pair
				//I cannot use a single pair and then changing
				//through the set method the two words, because the
				//map that stores all the pairs, are referenced
				//to the SAME pair; in that case, we will have all the pair
				//with the same string in first and second field.
				//So the string field's a pass by reference and not pass by value
			}

			//wordCount support phase
			currentBookTrace.setPunctNo(new IntWritable(punctNo));
			currentBookTrace.setFuncNo(new IntWritable(funcNo));	
			
		}//end map
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			//Extracting author name from filename
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			
			//Updating comments
			currentBookTrace.commenti+= "Book: " + filename + "\n";
			String daAgg =	"punctNo " + punctNo + "\n" +
							"funcNo " + funcNo + "\n" + 
							"Number of different words: " + currentBookTrace.getWordsArray().getArray().size() + "\n" +
							"Number of couples: " + currentBookTrace.getTwoGramsWritable().getTwoGrams().size() + "\n" +
							"Number of trigrams: " + currentBookTrace.getThreeGramsWritable().getThreeGrams().size() + "\n\n";
			currentBookTrace.commenti+= daAgg;
			context.write(new Text(filename), currentBookTrace);
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
