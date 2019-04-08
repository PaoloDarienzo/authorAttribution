package authorAttribution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class BookTrace implements Writable {
	
	private ArrayWritable wordVal;
	private IntWritable lineNo;
	//vicinanza...
	
	public BookTrace() {
		this.wordVal = new ArrayWritable();
		this.lineNo = new IntWritable(0);
	}
	
	public IntWritable getLineNo() {
		return this.lineNo;
	}
	
	public void setLineNo(IntWritable lineNo) {
		this.lineNo = lineNo;
	}
	
	public ArrayWritable getArray() {
		return this.wordVal;
	}
	
	public void setArray(ArrayWritable wordVal) {
		this.wordVal = wordVal;
	}
	
	public void addWord(String word) {
		this.wordVal.increment(word);
	}
	
	public void incrementLineNo() {
		this.lineNo = new IntWritable(this.lineNo.get() + 1);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.wordVal.readFields(in);
		this.lineNo.readFields(in);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		this.wordVal.write(out);	
		this.lineNo.write(out);
	}
	
	@Override
	public String toString() {
		String toReturn = "lineNo: " + this.lineNo + "\n";
		return toReturn + wordVal.toString();
	}

}
