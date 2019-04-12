package authorAttribution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

public class ThreeGramsWritable implements Writable {
	
	private HashMap<TextTrigram, Integer> threeGrams;
	
	public ThreeGramsWritable() {
		this.threeGrams = new HashMap<TextTrigram, Integer>();				
	}
	
	public HashMap<TextTrigram, Integer> getThreeGrams() {
		return this.threeGrams;
	}

	public void setThreeGrams(HashMap<TextTrigram, Integer> threeGrams) {
		this.threeGrams = threeGrams;
	}
	
	public void clear() {
		threeGrams.clear();
	}

	@Override
	public String toString() {
		//default:
		//TextTrigram.toString()=Integer.toString();
		//TextTrigram string:= (first, second, third)
		return threeGrams.toString();
	}

	public void sum(ThreeGramsWritable otherTrigram) {
		HashMap<TextTrigram, Integer> threeGramTemp = otherTrigram.getThreeGrams();
		
		for(Entry<TextTrigram, Integer> entry : threeGramTemp.entrySet()) {
			TextTrigram key = entry.getKey();
			if(this.threeGrams.containsKey(key)) {
				this.threeGrams.put(key, this.threeGrams.get(key) + entry.getValue());
			}
			else {
				this.threeGrams.put(key, entry.getValue());
			}
        }
	}
	
	public boolean containsKey(TextTrigram trigram) {
		return this.threeGrams.containsKey(trigram);
	}
	
	public void increment(TextTrigram newTrigram, int value) {
		int count = value;
		if(this.threeGrams.containsKey(newTrigram)) {
			count += this.threeGrams.get(newTrigram);
		}
		this.threeGrams.put(newTrigram, count);
	}
	
	public void increment(TextTrigram newTrigram) {
		this.increment(newTrigram, 1);		
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		threeGrams.clear();
		
		int size = in.readInt();
		for(int i = 0; i < size; i++) {
			
			TextTrigram key = new TextTrigram();
			key.readFields(in);
			
			Integer value = new Integer(in.readInt());
			
			this.threeGrams.put(key, value);
			
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(this.threeGrams.size());
		
		for(Entry<TextTrigram, Integer> entry : threeGrams.entrySet()) {
			TextTrigram key = entry.getKey();
			key.write(out);
        	
        	out.writeInt(entry.getValue());
        }
		
	}

}
