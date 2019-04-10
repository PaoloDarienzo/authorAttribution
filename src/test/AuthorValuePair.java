package test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class AuthorValuePair implements Writable {
	
	private Text author;
	HashMap<String, Integer> words = new HashMap<String, Integer>();
	
	public AuthorValuePair() {
		set(new Text(), new HashMap<String, Integer>());
	}

	public AuthorValuePair(Text author) {
		set(author, new HashMap<String, Integer>());
	}
	
	public AuthorValuePair(String author) {
		set(new Text(author), new HashMap<String, Integer>());
	}
	
	public AuthorValuePair(String author, HashMap<String, Integer> words) {
		set(new Text(author), words);
	}

	public AuthorValuePair(Text author, HashMap<String, Integer> words) {
		set(author, words);
	}
	
	public void set(Text author, HashMap<String, Integer> words) {
		this.author = author;
		this.words = words;
	}
	
	public void setAuthor(String author) {
		this.author = new Text(author);
	}
	
	public void setAuthor(Text author) {
		this.author = author;
	}

	public Text getAuthorText() {
		return author;
	}
	
	public String getAuthorString() {
		return author.toString();
	}
	
	public HashMap<String, Integer> getWords() {
		return this.words;
	}
	
	public void setWords(HashMap<String, Integer> words) {
		this.words = words;
	}
	
	public void clear() {
		this.words.clear();
	}
	
	public String authorToString() {
		return author.toString();
	}
	
	public String wordsToString() {
		return words.toString();
	}
	
	@Override
	public String toString() {
		return "(" + this.author.toString() + ", " + this.words.toString() + ")";
	}
	
	
	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(getAuthorString());
		
		out.writeInt(this.words.size());
		
		for(Entry<String, Integer> entry : words.entrySet()) {
			String key = entry.getKey();
        	out.writeInt(key.length());
        	out.writeBytes(key);   
        	out.writeInt(entry.getValue());
		}
		//l'importante e' l'ordine di scrittura simmetrico all'ordine di lettura
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		//deserializzazione
		
		this.author.readFields(in);
		
		words.clear();
		
		int size = in.readInt();
		
		for(int i = 0; i < size; i++) {
			
			int sizeKey = in.readInt();
			byte[] bytes = new byte[sizeKey];
			for(int j = 0; j < sizeKey; j++) {
				bytes[j] = in.readByte();
			}
			
			String key = new String(bytes);
			
			Integer value = in.readInt();
			
			this.words.put(key, value);
			
		}
		
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
		return author.hashCode() * 163 + words.hashCode();
	}
	
	public void sum(AuthorValuePair words) {
		
		HashMap<String, Integer> ht = words.getWords();
		
		for(Entry<String, Integer> entry : ht.entrySet()) {
			String key = entry.getKey();
			if(this.words.containsKey(key)) {
				this.words.put(key, this.words.get(key) + entry.getValue());
			}
			else {
				this.words.put(key, entry.getValue());
			}
        }	
	}
	
	public boolean containsKey(String otherWord) {
		return words.containsKey(otherWord);
	}
	
	//I metodi seguenti sono utilizzati nella versione del professore
	public void increment(String t, int value) {
		int count = value;
		if(words.containsKey(t)) {
			count += words.get(t) + count;
		}
		words.put(t, count);
	}
	
	public void increment(String t) {
		this.increment(t, 1);
	}
	
	public Set<Entry<String, Integer>> entrySet(){
		return words.entrySet();
	}

}
