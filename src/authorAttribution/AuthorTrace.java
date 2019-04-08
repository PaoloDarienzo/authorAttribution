package authorAttribution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class AuthorTrace implements Writable {
	
	private Text author;
	private BookTrace bookTraceFinal;
	private TreeMap<String, Integer> sorted;
	
	
	public AuthorTrace() {
		this.author = new Text();
		this.bookTraceFinal = new BookTrace();
		this.sorted = new TreeMap<>();
	}
	
	public void setAuthor(Text author) {
		this.author = author;
	}
	
	public void setWordsArray(ArrayWritable wordVal) {
		this.bookTraceFinal.setArray(wordVal);
	}
	
	public void setBookTrace(BookTrace bookTrace) {
		this.bookTraceFinal = bookTrace;
	}
	
	public BookTrace getBookTrace() {
		return this.bookTraceFinal;
	}
	
	public void setTree(TreeMap<String, Integer> sorted) {
		this.sorted = sorted;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.author.readFields(in);
		this.bookTraceFinal.readFields(in);
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		
		this.author.write(out);
		this.bookTraceFinal.write(out);
		
	}
	
	@Override
	public String toString() {
		
		String toReturn = "lineNo: " + this.bookTraceFinal.getLineNo().toString() + "\n";
		return toReturn + sorted.toString();
	
	}

}
