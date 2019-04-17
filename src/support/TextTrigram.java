package support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextTrigram implements WritableComparable<TextTrigram> {
	
	private Text first;
	private Text second;
	private Text third;
	
	public TextTrigram() {
		set(new Text(), new Text(), new Text());
	}
	
	public TextTrigram(String first, String second, String third) {
		set(new Text(first), new Text(second), new Text(third));
	}
	
	public TextTrigram(Text first, Text second, Text third) {
		set(first, second, third);
	}

	private void set(Text first, Text second, Text third) {
		this.setFirst(first);
		this.second = second;
		this.third = third;
	}

	public Text getFirst() {
		return this.first;
	}

	public void setFirst(Text first) {
		this.first = first;
	}
	
	public void setFirst(String first) {
		this.first = new Text(first);
	}
	
	public Text getSecond() {
		return this.second;
	}

	public void setSecond(Text second) {
		this.second = second;
	}
	
	public void setSecond(String second) {
		this.second = new Text(second);
	}
	
	public Text getThird() {
		return this.third;
	}

	public void setThird(Text third) {
		this.third = third;
	}
	
	public void setThird(String third) {
		this.third = new Text(third);
	}
	
	public String getFirstToString() {
		return this.first.toString();
	}
	
	public String getSecondToString() {
		return this.second.toString();
	}
	
	public String getThirdToString() {
		return this.third.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.first.write(out);
		this.second.write(out);
		this.third.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.first.readFields(in);
		this.second.readFields(in);
		this.third.readFields(in);
	}
	
	@Override
	public int hashCode() {
		return (first.hashCode() * 59 + this.second.hashCode()) * 101 + this.third.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		
		if(o instanceof TextTrigram) {
			TextTrigram tp = (TextTrigram) o;
			return first.equals(tp.first) && second.equals(tp.second) && third.equals(tp.third);
		}
		return false;
		
	}
	
	@Override
	public String toString() {
		return this.first.toString() + "|" + 
						this.second.toString() + "|" +
						this.third.toString();
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
	public int compareTo(TextTrigram tp) {
		
		int cmp = first.compareTo(tp.first);
		if (cmp != 0)
			return cmp;
		cmp = second.compareTo(tp.second);
		if (cmp != 0)
			return cmp;
		return third.compareTo(tp.third);
		
	}

}
