package support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

public class WordsFreqWritable implements Writable {
	
	private HashMap<String, Float> wordFreq;

	public WordsFreqWritable() {
		this.wordFreq = new HashMap<String, Float>();
	}
	
	public WordsFreqWritable(HashMap<String, Float> wordFreq) {
		this.wordFreq = wordFreq;
	}

	public HashMap<String, Float> getArray() {
		return this.wordFreq;
	}
	
	public void setArray(HashMap<String, Float> wordVal) {
		this.wordFreq = wordVal;
	}
	
	public void clear() {
		wordFreq.clear();
	}

	@Override
	public String toString() {
		String wordValToString = "";
		for(Entry<String, Float> entry : this.wordFreq.entrySet()) {
			String key = "@" + entry.getKey();
			wordValToString += key + "=" + entry.getValue() + "\n";
		}
		return wordValToString;
	}
	
	public void sum(WordsFreqWritable wordVal) {
		
		HashMap<String, Float> wordValTemp = wordVal.getArray();
		
		for(Entry<String, Float> entry : wordValTemp.entrySet()) {
			String key = entry.getKey();
			if(this.wordFreq.containsKey(key)) {
				this.wordFreq.put(key, this.wordFreq.get(key) + entry.getValue());
			}
			else {
				this.wordFreq.put(key, entry.getValue());
			}
        }
	}
	
	public boolean containsKey(String otherWord) {
		return wordFreq.containsKey(otherWord);
	}
	
	public void increment(String t, float value) {
		float count = value;
		if(wordFreq.containsKey(t)) {
			count += wordFreq.get(t);
		}
		wordFreq.put(t, count);
	}
	
	public void increment(String t) {
		this.increment(t, (float) 1.0);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		wordFreq.clear();
		
		int size = in.readInt();
		for(int i = 0; i < size; i++) {
			int sizeKey = in.readInt();
			byte[] bytes = new byte[sizeKey];
			for(int j = 0; j < sizeKey; j++) {
				bytes[j] = in.readByte();
			}
			String key = new String(bytes);
			Float value = in.readFloat();
			
			this.wordFreq.put(key, value);
			
		}
		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(this.wordFreq.size());
		
		for(Entry<String, Float> entry : wordFreq.entrySet()) {
			String key = entry.getKey();
        	out.writeInt(key.length());
        	out.writeBytes(key);
        	out.writeFloat(entry.getValue());
        }
		
	}

}