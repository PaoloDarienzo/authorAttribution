package test;
/*
 * Code from Tom White's "Hadoop the definitive guide"
 *
 * */

//cc TextPair A Writable implementation that stores a pair of Text objects
//cc TextPairComparator A RawComparator for comparing TextPair byte representations
//cc TextPairFirstComparator A custom RawComparator for comparing the first field of TextPair byte representations
//vv TextPair
import java.io.*;

import org.apache.hadoop.io.*;

/*
 * Tom White's "Hadoop the definitive guide"
 * */

public class JustPair implements WritableComparable<JustPair> {

	private String first;
	private String second;

	public JustPair() {}
	
	public JustPair(String first, String second) {
		set(first, second);
	}
	
	public void set(String first, String second) {
		this.first = first;
		this.second = second;
	}
	
	public void setFirst(String first) {
		this.first = first;
	}
	
	public void setSecond(String second) {
		this.second = second;
	}

	public String getFirst() {
		return this.first;
	}
	
	public String getSecond() {
		return this.second;
	}
	
	@Override
	public int hashCode() {
		return first.hashCode() * 59 + second.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		
		if(o instanceof JustPair) {
			JustPair tp = (JustPair) o;
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
	public int compareTo(JustPair tp) {
		
		int cmp = first.compareTo(tp.first);
		if (cmp != 0)
			return cmp;
		return second.compareTo(tp.second);
		
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
}
//^^ TextPair