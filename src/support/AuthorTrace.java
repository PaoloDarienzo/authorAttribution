package support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//import java.util.Map.Entry;
//import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class AuthorTrace implements WritableComparable<AuthorTrace> {
	
	private Text author;
	private IntWritable nBooks;
	private FloatWritable avgWordLength;
	//range 0-1, so percentage
	private FloatWritable punctuationDensity;
	//range 0-1, so percentage
	private FloatWritable functionDensity;
	private FloatWritable TTR;
	//private TreeMap<String, Integer> finalWordCountOrdered;
	private WordsArrayWritable finalWordCount;
	private TwoGramsWritable finalTwoGrams;
	private ThreeGramsWritable finalThreeGrams;
	
	//public String commenti = "";
	
	public AuthorTrace() {
		this.author = new Text();
		this.nBooks = new IntWritable(0);
		this.avgWordLength = new FloatWritable(0);
		this.punctuationDensity = new FloatWritable(0);
		this.functionDensity = new FloatWritable(0);
		this.setTTR(new FloatWritable(0));
		//this.finalWordCountOrdered = new TreeMap<>();
		this.finalWordCount = new WordsArrayWritable();
		this.setFinalTwoGrams(new TwoGramsWritable());
		this.setFinalThreeGrams(new ThreeGramsWritable());
	}
	
	public Text getAuthor() {
		return this.author;
	}
	
	public void setAuthor(Text author) {
		this.author = author;
	}
	
	public IntWritable getNBooks() {
		return this.nBooks;
	}
	
	public void setNBooks(IntWritable nBooks) {
		this.nBooks = nBooks;
	}
	
	public FloatWritable getAvgWordLength() {
		return this.avgWordLength;
	}
	
	public void setAvgWordLength(FloatWritable avgWordLength) {
		this.avgWordLength = avgWordLength;
	}
	
	public FloatWritable getPunctuationDensity() {
		return this.punctuationDensity;
	}
	
	public void setPunctuationDensity(FloatWritable punctuationDensity) {
		this.punctuationDensity = punctuationDensity;
	}
	
	public FloatWritable getFunctionDensity() {
		return this.functionDensity;
	}
	
	public void setFunctionDensity(FloatWritable FunctionDensity) {
		this.functionDensity = FunctionDensity;
	}
	
	public FloatWritable getTTR() {
		return this.TTR;
	}

	public void setTTR(FloatWritable TTR) {
		this.TTR = TTR;
	}
	
	/*
	public TreeMap<String, Integer> getTreeWordsArray() {
		return this.finalWordCountOrdered;
	}
	
	public void setTreeWordsArray(TreeMap<String, Integer> wordValOrdered) {
		this.finalWordCountOrdered = wordValOrdered;
	}
	*/

	public WordsArrayWritable getWordsArray(){
		return this.finalWordCount;
	}
	
	public void setWordsArray(WordsArrayWritable wordVal) {
		this.finalWordCount = wordVal;
	}
	
	public TwoGramsWritable getFinalTwoGrams() {
		return this.finalTwoGrams;
	}

	public void setFinalTwoGrams(TwoGramsWritable finalTwoGrams) {
		this.finalTwoGrams = finalTwoGrams;
	}
	
	public ThreeGramsWritable getFinalThreeGrams() {
		return this.finalThreeGrams;
	}
	
	public void setFinalThreeGrams(ThreeGramsWritable finalThreeGrams) {
		this.finalThreeGrams = finalThreeGrams;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.author.readFields(in);
		this.nBooks.readFields(in);
		this.avgWordLength.readFields(in);
		this.punctuationDensity.readFields(in);
		this.functionDensity.readFields(in);
		this.TTR.readFields(in);
		
		/*
		this.finalWordCountOrdered.clear();
		int size = in.readInt();
		for(int i = 0; i < size; i++) {
			int sizeKey = in.readInt();
			byte[] bytes = new byte[sizeKey];
			for(int j = 0; j < sizeKey; j++) {
				bytes[j] = in.readByte();
			}
			String key = new String(bytes);
			Integer value = new Integer(in.readInt());
			this.finalWordCountOrdered.put(key, value);
		}
		*/
		
		this.finalWordCount.readFields(in);		
		this.finalTwoGrams.readFields(in);
		this.finalThreeGrams.readFields(in);
		
		/*
		//commenti
		int sizeString = in.readInt();
		byte[] bytes = new byte[sizeString];
		for(int i = 0; i < sizeString; i++) {
			bytes[i] = in.readByte();
		}
		commenti = new String(bytes);
		*/
		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		
		this.author.write(out);
		this.nBooks.write(out);
		this.avgWordLength.write(out);
		this.punctuationDensity.write(out);
		this.functionDensity.write(out);
		this.TTR.write(out);
		
		/*
		out.writeInt(this.finalWordCountOrdered.size());
		for(Entry<String, Integer> entry : finalWordCountOrdered.entrySet()) {
			String key = entry.getKey();
        	out.writeInt(key.length());
        	out.writeBytes(key);   
        	out.writeInt(entry.getValue());
        }
		*/
		
		this.finalWordCount.write(out);
		this.finalTwoGrams.write(out);
		this.finalThreeGrams.write(out);
		
		/*
		//commenti
		out.writeInt(commenti.length());
    	out.writeBytes(commenti);
    	*/
		
	}
	
	@Override
	public String toString() {
		
		//Setting here the output to file for each author trace
		//actually it is set in AuthorAttribution, reduce phase
		return	"Author profile: " + this.author.toString() + "\n" +
				"(gen. from " + this.nBooks + " books)\n" + 
				"\n" + "Avg word length: " + this.avgWordLength +
				"\n" + "Punctuation words density: " + this.punctuationDensity +
				"\n" + "Function words density: " + this.functionDensity +
				"\n" + "TTR: " + this.TTR + 
				//Tree was used for WordCount ordered
				//"\n" + "WordCount: \n" + this.finalWordCountOrdered.toString() +
				"\n\n"+ "WordCount: \n" + this.finalWordCount.toString() +
				"\n" + "Couples: \n" + this.finalTwoGrams.toString() +
				"\n" + "Trigrams: \n" + this.finalThreeGrams.toString() + "\n";
		
	}

	@Override
	public int compareTo(AuthorTrace at) {
		
		//if two AuthorTrace are of the same author
		//i.e. author field is the same string,
		//then there are the same AuthorTrace
		
		return this.author.compareTo(at.author);
		
	}

}
