package test;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

//import org.apache.log4j.Logger;


/*
 * Quando verra' lanciato WordCount
 * WordCount <file input da analizzare, dir output dei res, #reducer>
 * Questi sono passati come args
 */

public class SplitTest extends Configured implements Tool {

	//private static final Logger LOG = Logger.getLogger(WordCount.class);
	
	public static void main(String[] args) throws Exception {
	  
		int res = ToolRunner.run(new SplitTest(), args);
		System.exit(res);
		}

	public int run(String[] args) throws Exception {
	  
		//wordcount e' l'etichetta del job: compare nell'interfaccia web
		Job job = Job.getInstance(getConf(), "SplitTest");
		job.setJarByClass(this.getClass());
		
		// Use TextInputFormat, the default unless job.setInputFormatClass is used
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Il resto e' roba di configurazione, per specificare nome classe ecc
		//Di default il num di reducer e' 1, se non chiedo il numero di reduce task
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//check del job, se il job sta girando o meno
		return job.waitForCompletion(true) ? 0 : 1;
    
  }

  //In Mapper<offset input, text riga letta, text parola singola, valore 1 che scriviamo>
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private final static Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		//private long numRecords = 0;
		
		private Text currentWord = new Text();

		/*
		* offset in realta' non la usiamo, ma usiamo soprattutto lineText
		* lineText e' una variabile di prov. Hadoop, ma per lavorarci la casto in line
		* essendo classe java posso lavorarci; 
		* posso utilizzare metodi es split (che lavorano pero' su stringhe)
		* con split spezzo riga del fine in token; le differenzio tramite pattern matching (classe Pattern)
		* il context.write corrisponde all'EMIT(word, 1)
		* 
		*/
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			
			String line = lineText.toString();
			//Text currentWord = new Text();
			context.write(new Text("line: " + line + "\n"), one);
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				//word.toLowerCase(); se voglio contare parole singole ignorando le maiuscole
				currentWord.set(word);
	            
				//EMIT(word, 1)
				//word e' tipo Text, 1 (one) e' di tipo IntWritable
				context.write(currentWord, one);
			}
		}
 
	}

	//In Reducer<chiaveingresso, valore, chiaveuscita, valore>
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				//get trasforma da IntWritable in int
				sum += count.get();
			}
			//EMIT(word, int(somma))
			context.write(word, new IntWritable(sum));
		}
		
	}

}
