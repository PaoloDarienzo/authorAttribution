package authorAttribution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class AuthorTrace implements Writable {
	
	private Text author;
	private TreeMap<String, Integer> finalWordValOrdered;
	private TwoGramsWritable finalTwoGrams;
	private FloatWritable avgWordLength;
	private FloatWritable punctuationDensity;
	private FloatWritable functionDensity;
	
	public String commenti = "";
	
	public AuthorTrace() {
		this.author = new Text();
		this.finalWordValOrdered = new TreeMap<>();
		this.setFinalTwoGrams(new TwoGramsWritable());
		this.avgWordLength = new FloatWritable(0);
		this.punctuationDensity = new FloatWritable(0);
		this.functionDensity = new FloatWritable(0);
	}
	
	public Text getAuthor() {
		return this.author;
	}
	
	public void setAuthor(Text author) {
		this.author = author;
	}
	
	public TreeMap<String, Integer> getTreeWordsArray() {
		return this.finalWordValOrdered;
	}
	
	public void setTreeWordsArray(TreeMap<String, Integer> finalWordValOrdered) {
		this.finalWordValOrdered = finalWordValOrdered;
	}
	
	public TwoGramsWritable getFinalTwoGrams() {
		return finalTwoGrams;
	}

	public void setFinalTwoGrams(TwoGramsWritable finalTwoGrams) {
		this.finalTwoGrams = finalTwoGrams;
	}

	public FloatWritable getAvgWordLength() {
		return this.avgWordLength;
	}
	
	public void setAvgWordLength(FloatWritable avgWordLength) {
		this.avgWordLength = avgWordLength;
	}
	
	public FloatWritable getPunctuationDensity() {
		return this.punctuationDensity;
	}
	
	public void setPunctuationDensity(FloatWritable punctuationDensity) {
		this.punctuationDensity = punctuationDensity;
	}
	
	public FloatWritable getFunctionDensity() {
		return this.functionDensity;
	}
	
	public void setFunctionDensity(FloatWritable FunctionDensity) {
		this.functionDensity = FunctionDensity;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.author.readFields(in);
		
		finalWordValOrdered.clear();
		int size = in.readInt();
		for(int i = 0; i < size; i++) {
			int sizeKey = in.readInt();
			byte[] bytes = new byte[sizeKey];
			for(int j = 0; j < sizeKey; j++) {
				bytes[j] = in.readByte();
			}
			String key = new String(bytes);
			Integer value = new Integer(in.readInt());
			this.finalWordValOrdered.put(key, value);
		}
		
		this.finalTwoGrams.readFields(in);
		this.avgWordLength.readFields(in);
		this.punctuationDensity.readFields(in);
		this.functionDensity.readFields(in);
		
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
		
		this.author.write(out);
		
		out.writeInt(this.finalWordValOrdered.size());
		
		for(Entry<String, Integer> entry : finalWordValOrdered.entrySet()) {
			String key = entry.getKey();
        	out.writeInt(key.length());
        	out.writeBytes(key);   
        	out.writeInt(entry.getValue());
        }
		
		this.finalTwoGrams.write(out);
		this.avgWordLength.write(out);
		this.punctuationDensity.write(out);
		this.functionDensity.write(out);
		
		//commenti
		out.writeInt(commenti.length());
    	out.writeBytes(commenti);
		
	}
	
	@Override
	public String toString() {
		//TODO
		//Setting here the output to file for each author trace
		//actually it is set in AuthorAttribution, reduce phase
		return this.commenti;
	
	}

}
