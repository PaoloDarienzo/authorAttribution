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
	private FloatWritable avgNoLine;
	private FloatWritable avgWordLength;
	private FloatWritable puntuactionDensity;
	private FloatWritable functionDensity;
	
	public String commenti = "";
	
	public AuthorTrace() {
		this.author = new Text();
		this.finalWordValOrdered = new TreeMap<>();
		this.avgNoLine = new FloatWritable(0);
		this.avgWordLength = new FloatWritable(0);
		this.puntuactionDensity = new FloatWritable(0);
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
	
	public FloatWritable getAvgNoLine() {
		return this.avgNoLine;
	}
	
	public void setAvgNoLine(FloatWritable avgNoLine) {
		this.avgNoLine = avgNoLine;
	}
	
	public FloatWritable getAvgWordLength() {
		return this.avgWordLength;
	}
	
	public void setAvgWordLength(FloatWritable avgWordLength) {
		this.avgWordLength = avgWordLength;
	}
	
	public FloatWritable getPuntuactionDensity() {
		return this.puntuactionDensity;
	}
	
	public void setPuntuactionDensity(FloatWritable puntuactionDensity) {
		this.puntuactionDensity = puntuactionDensity;
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
		
		this.avgNoLine.readFields(in);
		this.avgWordLength.readFields(in);
		this.puntuactionDensity.readFields(in);
		this.functionDensity.readFields(in);
		
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
		
		this.avgNoLine.write(out);
		this.avgWordLength.write(out);
		this.puntuactionDensity.write(out);
		this.functionDensity.write(out);
		
	}
	
	@Override
	public String toString() {
		
		String first = "commenti: " + this.commenti + "\n";
		
		String avgPhrases = "Average number of phrases: " + this.avgNoLine.toString() + "\n";
		String avgWordLength = "Average word length: " + this.avgWordLength.toString() + "\n";
		String avgPunt = "Puntuaction density: " + this.puntuactionDensity.toString() + "\n";
		String avgFunc = "Function words density:  " + this.functionDensity.toString() + "\n";
		
		return first + 
				avgPhrases + avgWordLength + avgPunt + avgFunc +
				finalWordValOrdered.toString();
	
	}

}
