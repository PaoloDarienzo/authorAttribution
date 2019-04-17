package support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

public class WordsArrayWritable implements Writable {
	
	private HashMap<String, Integer> wordVal;

	public WordsArrayWritable() {
		this.wordVal = new HashMap<String, Integer>();
	}
	
	public HashMap<String, Integer> getArray() {
		return this.wordVal;
	}
	
	public void setArray(HashMap<String, Integer> wordVal) {
		this.wordVal = wordVal;
	}
	
	public void clear() {
		wordVal.clear();
	}

	@Override
	public String toString() {
		String wordValToString = "";
		for(Entry<String, Integer> entry : this.wordVal.entrySet()) {
			String key = "@" + entry.getKey();
			wordValToString += key + "=" + entry.getValue() + "\n";
		}
		return wordValToString;
	}
	
	public void sum(WordsArrayWritable wordVal) {
		HashMap<String, Integer> wordValTemp = wordVal.getArray();
		
		for(Entry<String, Integer> entry : wordValTemp.entrySet()) {
			String key = entry.getKey();
			if(this.wordVal.containsKey(key)) {
				this.wordVal.put(key, this.wordVal.get(key) + entry.getValue());
			}
			else {
				this.wordVal.put(key, entry.getValue());
			}
        }
	}
	
	public boolean containsKey(String otherWord) {
		return wordVal.containsKey(otherWord);
	}
	
	public void increment(String t, int value) {
		int count = value;
		if(wordVal.containsKey(t)) {
			count += wordVal.get(t);
		}
		wordVal.put(t, count);
	}
	
	public void increment(String t) {
		this.increment(t, 1);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		wordVal.clear();
		
		int size = in.readInt();
		for(int i = 0; i < size; i++) {
			int sizeKey = in.readInt();
			byte[] bytes = new byte[sizeKey];
			for(int j = 0; j < sizeKey; j++) {
				bytes[j] = in.readByte();
			}
			String key = new String(bytes);
			Integer value = new Integer(in.readInt());
			
			this.wordVal.put(key, value);
			
		}
		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(this.wordVal.size());
		
		for(Entry<String, Integer> entry : wordVal.entrySet()) {
			String key = entry.getKey();
        	out.writeInt(key.length());
        	out.writeBytes(key);
        	out.writeInt(entry.getValue());
        }
		
	}

}