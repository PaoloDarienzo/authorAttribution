package test;

import java.util.HashMap;

public class PairHashTest {

	public static void main(String[] args) {
		
		HashMap<JustPair, Integer> pairs = new HashMap<JustPair, Integer>();
		
		JustPair firstPair = new JustPair("ciao", "hola");
		JustPair secondPair = new JustPair("hola", "ciao");
		JustPair thirdPair = new JustPair("ciao", "hola");
		
		if(pairs.containsKey(firstPair)) {
			pairs.put(firstPair, pairs.get(firstPair) + 1);
		}
		else {
			pairs.put(firstPair, 1);
		}
		
		if(pairs.containsKey(secondPair)) {
			pairs.put(firstPair, pairs.get(secondPair) + 1);
		}
		else {
			pairs.put(secondPair, 1);
		}
		
		if(pairs.containsKey(thirdPair)) {
			pairs.put(firstPair, pairs.get(thirdPair) + 1);
		}
		else {
			pairs.put(thirdPair, 1);
		}
		
		System.out.println(pairs.toString());
		
		if(pairs.containsKey(firstPair)) {
			pairs.put(firstPair, pairs.get(firstPair) + 1);
		}
		else {
			pairs.put(firstPair, 1);
		}
		
		if(pairs.containsKey(secondPair)) {
			pairs.put(secondPair, pairs.get(secondPair) + 1);
		}
		else {
			pairs.put(secondPair, 1);
		}
		
		if(pairs.containsKey(thirdPair)) {
			pairs.put(thirdPair, pairs.get(thirdPair) + 1);
		}
		else {
			pairs.put(thirdPair, 1);
		}
		
		System.out.println(pairs.toString());
		System.out.println(pairs.size());
		
		BookTraceNew tempBookTrace = new BookTraceNew();
		TwoGramWritableNew twoGrams = new TwoGramWritableNew();
		twoGrams.setTwoGrams(pairs);
		tempBookTrace.setTwoGrams(twoGrams);
		System.out.println(twoGrams.getTwoGrams().size());
		System.out.println(tempBookTrace.getTwoGrams().getTwoGrams().size());
		
	}
	
	public static class Pair {
		
		String first;
		String second;
		
		public Pair(String first, String second) {
			this.first = first;
			this.second = second;
		}
		
		public void setFirst(String first) {
			this.first = first;
		}
		
		public void setSecond(String second) {
			this.second = second;
		}
		
		public String getFirst() {return this.first;}
		public String getSecond() {return this.second;}
		
		@Override
		public String toString() {
			return "(" + this.first.toString() + ", " + this.second.toString() + ")";
		}
		
	    @Override    
	    public boolean equals(Object o) {        

	        if (this == o) return true;        

	        if (o == null || getClass() != o.getClass()) return false;        

	        Pair presunto = (Pair) o;        

	        if(!this.first.equals(presunto.first)) {
	        	return false;
	        }
	        else if(this.second.equals(presunto.second)) {
	        	return true;
	        }
	        return false;

	    }    

	    @Override    

	    public int hashCode() {      
	    	
	    	return this.first.hashCode() * this.second.hashCode();  

	    }    
		
	}

}
