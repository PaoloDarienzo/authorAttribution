package authorAttribution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import support.Unused;

public class BookTrace implements Writable {
	
	private WordsArrayWritable wordVal;
	private IntWritable puntNo;
	private IntWritable funcNo;
	private TwoGramsWritable twoGrams;
	//private int threeGram;
	
	public String commenti = "";
	
	public BookTrace() {
		this.wordVal = new WordsArrayWritable();
		this.puntNo = new IntWritable(0);
		this.funcNo = new IntWritable(0);
		this.twoGrams = new TwoGramsWritable();
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
	
	public void addPair(TextPair newPair) {
		this.twoGrams.increment(newPair);
	}

	public IntWritable getpuntNo() {
		return this.puntNo;
	}
	
	public void setpuntNo(IntWritable puntNo) {
		this.puntNo = puntNo;
	}
	
	@Unused
	public void incrementpuntNo() {
		this.puntNo = new IntWritable(this.puntNo.get() + 1);
	}
	
	public IntWritable getfuncNo() {
		return this.funcNo;
	}
	
	public void setfuncNo(IntWritable funcNo) {
		this.funcNo = funcNo;
	}
	
	@Unused
	public void incrementfuncNo() {
		this.funcNo = new IntWritable(this.funcNo.get() + 1);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.wordVal.readFields(in);
		this.puntNo.readFields(in);
		this.funcNo.readFields(in);
		this.twoGrams.readFields(in);
		
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
		this.puntNo.write(out);
		this.funcNo.write(out);
		this.twoGrams.write(out);
		
		//commenti
		out.writeInt(commenti.length());
    	out.writeBytes(commenti);
    	
	}
	
	@Override
	public String toString() {
		return 	"\n" + this.wordVal.toString() +"\n" + this.twoGrams.toString() + "\n";
	}

}
