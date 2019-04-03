package authorAttribution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import support.MethodsCollection;

public class TraceWord implements WritableComparable<TraceWord>  {
	
	private Text author;
	private Text word;
	//private IntWritable wordLength;
	private BooleanWritable isPuntuaction;
	private BooleanWritable isFunctionWord;
	
	//to instantiate otherwise MapReduce gives error
	public TraceWord() {
		set(new Text(), new Text());
	}

	public TraceWord(String author, String word) {
		set(new Text(author), new Text(word));
	}

	public TraceWord(Text author, Text word) {
		set(author, word);
	}
	
	public TraceWord(Text author, String word) {
		set(author, new Text(word));
	}
	
	public TraceWord(String author, Text word) {
		set(new Text(author), word);
	}
	
	public TraceWord(Text author) {
		set(author, new Text());
	}
	
	public TraceWord(String author) {
		set(new Text(author), new Text());
	}
	
	public void set(String author, String word) {
		set(new Text(author), new Text(word));
	}
	
	public void set(Text author, Text word) {
		this.author = author;
		this.word = word;
	}

	public void setAuthor(Text author) {
		this.author = author;
	}
	
	public Text getAuthor() {
		return this.author;
	}

	public String getAuthorToString() {
		return this.author.toString();
	}

	public Text getWord() {
		return word;
	}
	
	public void setWord(Text word) {
		setWord(word);
	}

	public void setWord(String word) {
		this.word = new Text(word);
		this.isFunctionWord = new BooleanWritable(MethodsCollection.functionWordChecker(word));
		this.isPuntuaction = new BooleanWritable(MethodsCollection.puntuactionChecker(word));
		//this.wordLength = new IntWritable(word.length());
	}
	
	public IntWritable getWordLength() {
		return new IntWritable(this.word.toString().length());
	}

	/*
	public void setWordLength(IntWritable wordLength) {
		this.wordLength = wordLength;
	}
	*/
	
	/*
	public void setWordLength(int wordLength) {
		this.wordLength = new IntWritable(wordLength);
	}
	*/

	public boolean isPuntuaction() {
		return isPuntuaction.get();
	}

	public void setPuntuaction(boolean isPuntuaction) {
		this.isPuntuaction = new BooleanWritable(isPuntuaction);
	}

	public boolean isFunctionWord() {
		return isFunctionWord.get();
	}

	public void setFunctionWord(boolean isFunctionWord) {
		this.isFunctionWord = new BooleanWritable(isFunctionWord);
	}

	
	@Override
	public String toString() {
		return "(" + this.author.toString() + ", " + this.word.toString() + ")";
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		this.author.write(out);
		this.word.write(out);
		//this.wordLength.write(out);
		
		String booleanPuntuaction = new Boolean(this.isPuntuaction()).toString();
		out.writeInt(booleanPuntuaction.length());
		out.writeBytes(booleanPuntuaction);
		//this.isPuntuaction.write(out);
		//this.isFunctionWord.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.author.readFields(in);
		this.word.readFields(in);
		//this.wordLength.readFields(in);
		
		int sizeBoolPunt = in.readInt();
		byte[] bytes = new byte[sizeBoolPunt];
		for(int j = 0; j < sizeBoolPunt; j++) {
			bytes[j] = in.readByte();
		}
		String booleanPuntuaction = new String(bytes);
		this.isPuntuaction = new BooleanWritable(new Boolean(booleanPuntuaction).booleanValue());
		//this.isPuntuaction.readFields(in);
		//this.isFunctionWord.readFields(in);
	}
	
	@Override
	public int hashCode() {
		//i coeff della combinazione lineare devono essere numeri primi
		return author.hashCode() * 163 + word.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		
		if(o instanceof TraceWord) {
			TraceWord curT = (TraceWord) o;
			return this.author.equals(curT.author) && this.word.equals(word);
		}
		return false;
		
	}

	/*
	*	-1: this.object  <  tp
	*	 0: this.object  == tp
	*	 1: this.object  >  tp
	*/
	/**
	 * Mi appoggio al compare dei singoli componenti del CurrentTraceWord
	 * @return
	 * a negative integer, zero, or a positive integer as 
	 * this object is less than, equal to, or greater than the specified object.
	 */
	@Override
	public int compareTo(TraceWord curT) {
		
		int cmp = this.author.compareTo(curT.author);
		if (cmp != 0)
			return cmp;
		return this.word.compareTo(curT.word);
		
	}

}
