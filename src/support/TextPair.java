package support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/*
 * Code from Tom White's "Hadoop the definitive guide"
 *
 * */

//cc TextPair A Writable implementation that stores a pair of Text objects
//cc TextPairComparator A RawComparator for comparing TextPair byte representations
//cc TextPairFirstComparator A custom RawComparator for comparing the first field of TextPair byte representations
//vv TextPair

/*
 * Tom White's "Hadoop the definitive guide"
 * */

public class TextPair implements WritableComparable<TextPair> {

	private Text first;
	private Text second;

	public TextPair() {
		set(new Text(), new Text());
	}

	public TextPair(String first, String second) {
		set(new Text(first), new Text(second));
	}

	public TextPair(Text first, Text second) {
		set(first, second);
	}
	
	public void set(String first, String second) {
		set(new Text(first), new Text(second));
	}
	
	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}
	
	public void setFirst(Text first) {
		this.first = first;
	}
	
	public void setFirst(String first) {
		this.first = new Text(first);
	}
	
	public void setSecond(Text second) {
		this.second = second;
	}
	
	public void setSecond(String second) {
		this.second = new Text(second);
	}

	public Text getFirst() {
		return this.first;
	}
	
	public Text getSecond() {
		return this.second;
	}
	
	public String getFirstToString() {
		return this.first.toString();
	}
	
	public String getSecondToString() {
		return this.second.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.first.write(out);
		this.second.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.first.readFields(in);
		this.second.readFields(in);
	}
	
	@Override
	public int hashCode() {
		return this.first.hashCode() * 59 + this.second.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		
		if(o instanceof TextPair) {
			TextPair tp = (TextPair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
		
	}
	
	@Override
	public String toString() {
		return "(" + this.first.toString() + ", " + this.second.toString() + ")";
	}
	
	/*
	*	-1: this.object  <  tp
	*	 0: this.object  == tp
	*	 1: this.object  >  tp
	*/
	/**
	 * Mi appoggio al compare dei singoli componenti del Pair
	 * @return
	 * a negative integer, zero, or a positive integer as 
	 * this object is less than, equal to, or greater than the specified object.
	 */
	@Override
	public int compareTo(TextPair tp) {
		
		int cmp = first.compareTo(tp.first);
		if (cmp != 0)
			return cmp;
		return second.compareTo(tp.second);
		
	}
	
}
//^^ TextPair