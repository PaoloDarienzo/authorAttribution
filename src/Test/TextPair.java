package Test;
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
	
	public TextPair(Text first, String second) {
		set(first, new Text(second));
	}
	
	public TextPair(String first, Text second) {
		set(new Text(first), second);
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
		//serializzazione; come scrivere byte su disco
		//es: context.write chiama tale metodo
		this.first.write(out);
		this.second.write(out);
		//l'importante e' l'ordine di scrittura simmetrico all'ordine di lettura
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		//deserializzazione
		this.first.readFields(in);
		this.second.readFields(in);
	}
	
	@Override
	public int hashCode() {
		/*
		 * hash(w)mod#R; per 1 word sola (mod#R fatto dal sistema)
		 * 
		 * hash(i, j) ?
		 * 
		 */
		//return Objects.hash(this.first, this.second);	
		
		//Doing hash on first elem, it allows research based on first elem,
		//allowing efficiency on normalization
		//Fare hash sul primo elemento, mi assicuro che il partitioner mandi pair
		//con prima parola uguale sullo stesso reducer; ma e' poco efficiente
		//e non generico (mi serve poche volte)
		//return this.first.hashCode();
		
		//i coeff della combinazione lineare devono essere numeri primi
		return first.hashCode() * 163 + second.hashCode();
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