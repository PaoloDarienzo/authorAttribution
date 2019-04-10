package test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

public class TwoGramWritableNew implements Writable {
	
	private HashMap<JustPair, Integer> twoGrams;
	
	public TwoGramWritableNew() {
		this.twoGrams = new HashMap<JustPair, Integer>();				
	}

	public HashMap<JustPair, Integer> getTwoGrams() {
		return this.twoGrams;
	}

	public void setTwoGrams(HashMap<JustPair, Integer> twoGrams) {
		this.twoGrams = twoGrams;
	}
	
	public void clear() {
		twoGrams.clear();
	}

	@Override
	public String toString() {
		//default:
		//TextPair.toString()=Integer.toString();
		//TextPair string:= (first, second)
		return twoGrams.toString();
	}
	
	public void sum(TwoGramWritableNew otherTwoGram) {
		HashMap<JustPair, Integer> twoGramTemp = otherTwoGram.getTwoGrams();
		
		for(Entry<JustPair, Integer> entry : twoGramTemp.entrySet()) {
			JustPair key = entry.getKey();
			if(this.twoGrams.containsKey(key)) {
				this.twoGrams.put(key, this.twoGrams.get(key) + entry.getValue());
			}
			else {
				this.twoGrams.put(key, entry.getValue());
			}
        }
	}
	
	public boolean containsKey(JustPair pair) {
		return this.twoGrams.containsKey(pair);
	}
	
	public void increment(JustPair newPair, int value) {
		int count = value;
		if(this.twoGrams.containsKey(newPair)) {
			count += this.twoGrams.get(newPair);
		}
		this.twoGrams.put(newPair, count);
	}
	
	public void increment(JustPair newPair) {
		this.increment(newPair, 1);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		twoGrams.clear();
		
		int size = in.readInt();
		for(int i = 0; i < size; i++) {
			
			JustPair key = new JustPair();
			key.readFields(in);
			
			Integer value = new Integer(in.readInt());
			
			this.twoGrams.put(key, value);
			
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(this.twoGrams.size());
		
		for(Entry<JustPair, Integer> entry : twoGrams.entrySet()) {
			JustPair key = entry.getKey();
			key.write(out);
        	
        	out.writeInt(entry.getValue());
        }
		
	}

} //end twoGramWritable class
