package support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

public class TwoGramsWritable implements Writable {
	
	private HashMap<TextPair, Integer> twoGrams;
	
	public TwoGramsWritable() {
		this.twoGrams = new HashMap<TextPair, Integer>(128);				
	}
	
	public TwoGramsWritable(int supposedSize) {
		this.twoGrams = new HashMap<TextPair, Integer>(supposedSize);				
	}
	
	public TwoGramsWritable(int supposedSize, float loadFact) {
		this.twoGrams = new HashMap<TextPair, Integer>(supposedSize, loadFact);				
	}

	public TwoGramsWritable(HashMap<TextPair, Integer> twoGrams) {
		this.twoGrams = twoGrams;				
	}

	public HashMap<TextPair, Integer> getTwoGrams() {
		return this.twoGrams;
	}

	public void setTwoGrams(HashMap<TextPair, Integer> twoGrams) {
		this.twoGrams = twoGrams;
	}
	
	public void clear() {
		this.twoGrams.clear();
	}

	@Override
	public String toString() {
		String wordValToString = "";
		for(Entry<TextPair, Integer> entry : this.twoGrams.entrySet()) {
			String key = "%" + entry.getKey();
			wordValToString += key + "=" + entry.getValue() + "\n";
		}
		return wordValToString;
	}
	
	public void sum(TwoGramsWritable otherTwoGram) {
		HashMap<TextPair, Integer> twoGramTemp = otherTwoGram.getTwoGrams();
		
		for(Entry<TextPair, Integer> entry : twoGramTemp.entrySet()) {
			TextPair key = entry.getKey();
			if(this.twoGrams.containsKey(key)) {
				this.twoGrams.put(key, this.twoGrams.get(key) + entry.getValue());
			}
			else {
				this.twoGrams.put(key, entry.getValue());
			}
        }
	}
	
	public boolean containsKey(TextPair pair) {
		return this.twoGrams.containsKey(pair);
	}
	
	public void increment(TextPair newPair, int value) {
		int count = value;
		if(this.twoGrams.containsKey(newPair)) {
			count += this.twoGrams.get(newPair);
		}
		this.twoGrams.put(newPair, count);
	}
	
	public void increment(TextPair newPair) {
		this.increment(newPair, 1);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		twoGrams.clear();
		
		int size = in.readInt();
		for(int i = 0; i < size; i++) {
			
			TextPair key = new TextPair();
			key.readFields(in);
			
			Integer value = new Integer(in.readInt());
			
			this.twoGrams.put(key, value);
			
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(this.twoGrams.size());
		
		for(Entry<TextPair, Integer> entry : twoGrams.entrySet()) {
			TextPair key = entry.getKey();
			key.write(out);
        	
        	out.writeInt(entry.getValue());
        }
		
	}

} //end twoGramWritable class
