package authorAttribution;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import org.apache.log4j.Logger;

/*
 * H:= associativeArray
 * HashMap, TreeMap, or LinkedHashMap?
 * TreeMap e' ordinato; O(log(n))
 * HashMap: O(1)
 */

public class WordCountInMem extends Configured implements Tool {

	//private static final Logger LOG = Logger.getLogger(WordCountInMem.class);

	public static void main(String[] args) throws Exception {
		  
		int res = ToolRunner.run(new WordCountInMem(), args);
		System.exit(res);
	}
	
	public int run(String[] args) throws Exception {
		  
		//wordcountinmem e' l'etichetta del job: compare nell'interfaccia web
		Job job = Job.getInstance(getConf(), "wordcountinmem");
		job.setJarByClass(this.getClass());
		
		// Use TextInputFormat, the default unless job.setInputFormatClass is used
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Il resto e' roba di configurazione, per specificare nome classe ecc
		//Di default il num di reducer e' 1
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		  
		//check del job, se il job sta girando o meno
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		
		//private Text word = new Text();
		//private long numRecords = 0;    
		
		private static HashMap<String, Integer> H;
		private Text currentWord = new Text();
		private IntWritable currentValue = new IntWritable();
		
		protected void setup(Context context) {
			Map.H = new HashMap<String, Integer>();
		}

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			
			String line = lineText.toString();
			
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				
				if(H.containsKey(word)) {
					H.put(word, H.get(word) + 1);
				}
				else
					H.put(word, 1);
		     
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			
			/*
			//SLOWER THAN THE OTHER ITERATING METHOD!
			for(String term : H.keySet()) {
				currentWord.set(term);
				context.write(currentWord, new IntWritable(H.get(term)));
			}
			*/
			
			for(Entry<String, Integer> entry : H.entrySet()) {
	        	currentWord.set(entry.getKey());
	        	currentValue.set(entry.getValue());
	        	context.write(currentWord, currentValue);
	        }
			
		}
	
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			context.write(word, new IntWritable(sum));
		}
	}
	
}
