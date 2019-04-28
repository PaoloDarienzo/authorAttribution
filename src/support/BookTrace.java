package support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class BookTrace implements Writable {
	
	private IntWritable punctNo; //number of punctuation words
	private IntWritable funcNo; //number of function words
	private WordsArrayWritable wordCount;
	private TwoGramsWritable twoGrams;
	private ThreeGramsWritable threeGrams;
	
	//public String commenti = "";
	
	public BookTrace() {
		this.punctNo = new IntWritable(0);
		this.funcNo = new IntWritable(0);
		this.wordCount = new WordsArrayWritable();
		this.twoGrams = new TwoGramsWritable();
		this.threeGrams = new ThreeGramsWritable();
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
	
	public WordsArrayWritable getWordsArray() {
		return this.wordCount;
	}
	
	public void setWordsArray(WordsArrayWritable wordVal) {
		this.wordCount = wordVal;
	}
	
	public void addWord(String word) {
		this.wordCount.increment(word);
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
	
	public ThreeGramsWritable getThreeGramsWritable() {
		return this.threeGrams;
	}

	public void setThreeGramsWritable(ThreeGramsWritable threeGrams) {
		this.threeGrams = threeGrams;
	}
	
	public void addTrigram(TextTrigram newTrigram) {
		this.threeGrams.increment(newTrigram);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.punctNo.readFields(in);
		this.funcNo.readFields(in);
		this.wordCount.readFields(in);
		this.twoGrams.readFields(in);
		this.threeGrams.readFields(in);
		
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
		this.punctNo.write(out);
		this.funcNo.write(out);
		this.wordCount.write(out);
		this.twoGrams.write(out);
		this.threeGrams.write(out);
		
		/*
		//commenti
		out.writeInt(commenti.length());
    	out.writeBytes(commenti);
    	*/
    	
	}
	
	@Override
	public String toString() {
		return 	"\n" + 	this.wordCount.toString() +"\n" +
						this.twoGrams.toString() + "\n" +
						this.threeGrams.toString() + "\n";
	}

}
