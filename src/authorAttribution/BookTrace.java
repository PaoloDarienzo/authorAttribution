package authorAttribution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import support.Unused;

public class BookTrace implements Writable {
	
	private WordsArrayWritable wordVal;
	private IntWritable punctNo; //number of punctuation words
	private IntWritable funcNo; //number of function words
	private TwoGramsWritable twoGrams;
	private ThreeGramsWritable threeGrams;
	
	public String commenti = "";
	
	public BookTrace() {
		this.wordVal = new WordsArrayWritable();
		this.punctNo = new IntWritable(0);
		this.funcNo = new IntWritable(0);
		this.twoGrams = new TwoGramsWritable();
		this.threeGrams = new ThreeGramsWritable();
	}
	
	public WordsArrayWritable getWordsArray() {
		return this.wordVal;
	}
	
	public void setWordsArray(WordsArrayWritable wordVal) {
		this.wordVal = wordVal;
	}
	
	public void addWord(String word) {
		this.wordVal.increment(word);
	}
	
	public TwoGramsWritable getTwoGramsWritable() {
		return this.twoGrams;
	}

	public void setTwoGramsWritable(TwoGramsWritable twoGrams) {
		this.twoGrams = twoGrams;
	}
	
	public ThreeGramsWritable getThreeGramsWritable() {
		return this.threeGrams;
	}

	public void setThreeGramsWritable(ThreeGramsWritable threeGrams) {
		this.threeGrams = threeGrams;
	}
	
	public void addPair(TextPair newPair) {
		this.twoGrams.increment(newPair);
	}
	
	public void addTrigram(TextTrigram newTrigram) {
		this.threeGrams.increment(newTrigram);
	}

	public IntWritable getPunctNo() {
		return this.punctNo;
	}
	
	public void setPunctNo(IntWritable punctNo) {
		this.punctNo = punctNo;
	}
	
	@Unused
	public void incrementPunctNo() {
		this.punctNo = new IntWritable(this.punctNo.get() + 1);
	}
	
	public IntWritable getFuncNo() {
		return this.funcNo;
	}
	
	public void setFuncNo(IntWritable funcNo) {
		this.funcNo = funcNo;
	}
	
	@Unused
	public void incrementFuncNo() {
		this.funcNo = new IntWritable(this.funcNo.get() + 1);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.wordVal.readFields(in);
		this.punctNo.readFields(in);
		this.funcNo.readFields(in);
		this.twoGrams.readFields(in);
		this.threeGrams.readFields(in);
		
		//commenti
		int sizeString = in.readInt();
		byte[] bytes = new byte[sizeString];
		for(int i = 0; i < sizeString; i++) {
			bytes[i] = in.readByte();
		}
		commenti = new String(bytes);
			
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		this.wordVal.write(out);
		this.punctNo.write(out);
		this.funcNo.write(out);
		this.twoGrams.write(out);
		this.threeGrams.write(out);
		
		//commenti
		out.writeInt(commenti.length());
    	out.writeBytes(commenti);
    	
	}
	
	@Override
	public String toString() {
		return 	"\n" + 	this.wordVal.toString() +"\n" +
						this.twoGrams.toString() + "\n" +
						this.threeGrams.toString() + "\n";
	}

}
