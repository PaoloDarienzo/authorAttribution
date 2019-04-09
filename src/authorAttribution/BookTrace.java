package authorAttribution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import support.Unused;

public class BookTrace implements Writable {
	
	private ArrayWritable wordVal;
	private IntWritable lineNo;
	private IntWritable puntNo;
	private IntWritable funcNo;
	//vicinanza...
	
	public String commenti = "";
	
	public BookTrace() {
		this.wordVal = new ArrayWritable();
		this.lineNo = new IntWritable(0);
		this.puntNo = new IntWritable(0);
		this.funcNo = new IntWritable(0);
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
	
	public IntWritable getLineNo() {
		return this.lineNo;
	}
	
	public void setLineNo(IntWritable lineNo) {
		this.lineNo = lineNo;
	}
	
	@Unused
	public void incrementLineNo() {
		this.lineNo = new IntWritable(this.lineNo.get() + 1);
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
		this.lineNo.readFields(in);
		this.puntNo.readFields(in);
		this.funcNo.readFields(in);
		
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
		this.lineNo.write(out);
		this.puntNo.write(out);
		this.funcNo.write(out);
		
		out.writeInt(commenti.length());
    	out.writeBytes(commenti);
    	
	}
	
	@Override
	public String toString() {
		
		String lineNo = "lineNo: " + this.lineNo + "\n";
		String puntNo = "puntNo: " + this.puntNo + "\n";
		String funcNo = "funcNo: " + this.funcNo + "\n";
		
		return 	"\nCommenti del libro: " + commenti + "\n" + 
				lineNo + puntNo + funcNo + wordVal.toString();
	}

}
