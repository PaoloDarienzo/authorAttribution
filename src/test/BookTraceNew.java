package test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class BookTraceNew implements Writable {
	
	private TwoGramWritableNew twoGrams;
	//private int threeGram;
	
	public String commenti = "";
	
	public BookTraceNew() {
		this.twoGrams = new TwoGramWritableNew();
	}
	
	public TwoGramWritableNew getTwoGrams() {
		return this.twoGrams;
	}

	public void setTwoGrams(TwoGramWritableNew twoGrams) {
		this.twoGrams = twoGrams;
	}
	
	public void addPair(JustPair newPair) {
		this.twoGrams.increment(newPair);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
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
		
		this.twoGrams.write(out);
		
		//commenti
		out.writeInt(commenti.length());
    	out.writeBytes(commenti);
    	
	}
	
	@Override
	public String toString() {
		return 	"\n" + this.twoGrams.toString() + "\n";
	}

}
