package support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
	private WordsFreqWritable wordFreqKey;
	private TwoGramsWritable twoGramsKey;
	private ThreeGramsWritable threeGramsKey;
	
	//public String commenti = "";
	
	public AuthorTrace() {
		this.author = new Text();
		this.nBooks = new IntWritable(0);
		this.avgWordLength = new FloatWritable(0);
		this.punctuationDensity = new FloatWritable(0);
		this.functionDensity = new FloatWritable(0);
		this.setTTR(new FloatWritable(0));
		this.wordFreqKey = new WordsFreqWritable();
		this.setTwoGramsKey(new TwoGramsWritable());
		this.setThreeGramsKey(new ThreeGramsWritable());
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

	public WordsFreqWritable getWordsFreqArray(){
		return this.wordFreqKey;
	}
	
	public void setWordsFreqArray(WordsFreqWritable wordVal) {
		this.wordFreqKey = wordVal;
	}
	
	public TwoGramsWritable getTwoGramsKey() {
		return this.twoGramsKey;
	}

	public void setTwoGramsKey(TwoGramsWritable twoGramsKey) {
		this.twoGramsKey = twoGramsKey;
	}
	
	public ThreeGramsWritable getThreeGramsKey() {
		return this.threeGramsKey;
	}
	
	public void setThreeGramsKey(ThreeGramsWritable threeGramsKey) {
		this.threeGramsKey = threeGramsKey;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.author.readFields(in);
		this.nBooks.readFields(in);
		this.avgWordLength.readFields(in);
		this.punctuationDensity.readFields(in);
		this.functionDensity.readFields(in);
		this.TTR.readFields(in);
		
		this.wordFreqKey.readFields(in);		
		this.twoGramsKey.readFields(in);
		this.threeGramsKey.readFields(in);
		
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
		
		this.wordFreqKey.write(out);
		this.twoGramsKey.write(out);
		this.threeGramsKey.write(out);
		
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
				"\n\n"+ "WordFreq: \n" + this.wordFreqKey.toString() +
				"\n" + "Couples: \n" + this.twoGramsKey.toString() +
				"\n" + "Trigrams: \n" + this.threeGramsKey.toString() + "\n";
		
	}

	@Override
	public int compareTo(AuthorTrace at) {
		
		//if two AuthorTrace are of the same author
		//i.e. author field is the same string,
		//then there are the same AuthorTrace
		
		return this.author.compareTo(at.author);
		
	}

}
