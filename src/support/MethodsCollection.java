package support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.FloatWritable;

/**
 * Collection of methods used for Author Attribution
 * @author Paolo D'Arienzo
 *
 */
public final class MethodsCollection {
	
	public static final String[] SET_PUNCT_VALUES = new String[]{
																	".", ",", ":", ";",
																	"?", "!", "(", ")",
																	"-", "\""
																};
	public static final Set<String> PUNCTUATION = new HashSet<>(Arrays.asList(SET_PUNCT_VALUES));
	
	public static final String[] SET_FUNCTION_WORDS = new String[] 
			{
				"a", "between", "in", "nor", "some", "upon", "about", "both",
				"including", "nothing", "somebody", "us", "above", "but", "inside",
				"of", "someone", "used", "after", "by", "into", "off", "something",
				"via", "all", "can", "is", "on", "such", "we", "although", "coos", "it",
				"once", "than", "what", "am", "do", "its", "one", "that", "whatever", "among",
				"down", "latter", "onto", "the", "when", "an", "each", "less",
				"opposite", "their", "where", "and", "either", "like", "or", "them", "whether",
				"another", "enough", "little", "our", "these", "which", "any", "every", "lots", "outside",
				"they", "while", "anybody", "everybody", "many", "over", "this", "who", "anyone",
				"everyone", "me", "own", "those", "whoever", "anything", "everything", "more", "past",
				"though", "whom", "are", "few", "most", "per", "though", "whose", "around", "following", 
				"much", "plenty", "till", "will", "as", "for", "must", "plus", "to", "with", "at", 
				"from", "my", "regarding", "toward", "within", "be", "have", "near", "same", "towards",
				"without", "because", "he", "need", "several", "under", "worth", "before", "her",
				"neither", "she", "unless", "would", "behind", "him", "no", "should", "unlike", "yes",
				"below", "I", "nobody", "since", "until", "you", "beside", "if", "none"
			};
	
	public static final Set<String> FUNCTION_WORDS = new HashSet<>(Arrays.asList(SET_FUNCTION_WORDS));

	/**
	 * 
	 * @param word
	 * @return true if word is a punctuation symbol, false otherwise
	 */
	public static boolean punctuationChecker(String word) {
		
		if(PUNCTUATION.contains(word)) {
			return true;
		}
		else {
			return false;
		}
	}

	/**
	 * 
	 * @param word
	 * @return true if word is a fuction word, false otherwise
	 */
	public static boolean functionWordChecker(String word) {

		if(FUNCTION_WORDS.contains(word)) {
			return true;
		}
		else {
			return false;
		}
		
	}
	
	/**
	 * 
	 * @param wordVal HashMap containing word as key, number of time the word is used as value
	 * @return the total number of chars inside the HashMap, excluding punctuation
	 */
	public static int getTotalCharsOnWords(HashMap<String, Integer> wordVal) {
		
		int totalChars = 0;
				
		for (Entry<String, Integer> entry : wordVal.entrySet()) {
		    String key = entry.getKey();
		    Integer value = entry.getValue();
		    //excludes punctuation
		    if(!MethodsCollection.punctuationChecker(key)) {
		    	totalChars += key.length() * value.intValue();	
		    }
		}
		
		return totalChars;
		
	}

	/**
	 * 
	 * @param wordVal HashMap containing word as key, number of time the word is used as value
	 * @return the number of total entries, multiplied for their value, including punctuation
	 */
	public static int getTotalWords(HashMap<String, Integer> wordVal) {
		//counts punctuation too
		int totalWords = 0;
		
		for (Entry<String, Integer> entry : wordVal.entrySet()) {
		    Integer value = entry.getValue();
		    totalWords += value.intValue();
		}
		
		return totalWords;
		
	}
	
	/**
	 * 
	 * @param wordVal HashMap containing word as key, number of time the word is used as value
	 * @return the number of total entries, multiplied for their value, excluding punctuation
	 */
	public static int getTotalWordsWOPunct(HashMap<String, Integer> wordVal) {
		//excludes punctuation
		int totalWords = 0;
		
		for (Entry<String, Integer> entry : wordVal.entrySet()) {
			if(!MethodsCollection.punctuationChecker(entry.getKey())) {
				Integer value = entry.getValue();
			    totalWords += value.intValue();
			}
		}
		
		return totalWords;
		
	}
	
	/**
	 * 
	 * @param unkFirst
	 * @param knownSecond
	 * @return the distance between the two float in percentage
	 */
	public static float getFloatRatio(FloatWritable unkFirst, FloatWritable knownSecond) {
		return getFloatRatio(unkFirst.get(), knownSecond.get());
	}
	
	/**
	 * 
	 * @param a
	 * @param b
	 * @return the distance between the two float in percentage
	 */
	public static float getFloatRatio(float a, float b) {
		/*
		It returns how much two numbers a, b are similar, in percentage
		(0 are completely different, 100 are the same).
		If a or b are 0, they are substituted to the epsilon value 0.0001.
		If they are both 0, they are equal, so it returns 0.
		*/
		float maxVal, ratioToCalc, x;
		
		if(a == b) { //covers 0 case
			return 100;
		}
		
		else { //a and b are always positive
			assert a >= 0;
			assert b >= 0;
			
			if (a > b) {
				if (b == 0) {
					//0 becomes epsilon
					b = (float) 0.0001;
				}
				maxVal = a;
				ratioToCalc = b;
				//ratioToCalc : maxVal = x : 100
				x = ratioToCalc * 100 / maxVal;
				return x;
			}
			else {
				if (a == 0) {	
					//0 becomes epsilon
					a = (float) 0.0001;
				}
				maxVal = b;
				ratioToCalc = a;
				//ratioToCalc : maxVal = x : 100
				x = ratioToCalc * 100 / maxVal;
				return x;
			}
		}
		
	}
	
	/**
	 * 
	 * @param wordCount HashMap containing the wordCount
	 * @return the TTR, i.e. type token ratio
	 */
	public static float getTTR(HashMap<String, Integer> wordCount) {
		
		float numDiffWords = (float) getWordsOnlySize(wordCount);
		float numTotalWords = (float) getTotalWordsWOPunct(wordCount);
		
		return ((float) numDiffWords / (float) numTotalWords);
	}
	
	/**
	 * 
	 * @param wordCount HashMap containing the wordCount
	 * @return the number of keys of wordCount without punctuation symbols, i.e. the number of different words
	 */
	public static int getWordsOnlySize(HashMap<String, Integer> wordCount) {
		
		int size = wordCount.size();
		
		String[] set_punct_values = new String[] {	".", ",", ":", ";",
													"?", "!", "(", ")",
													"-", "\""};
		Set<String> punctuation = new HashSet<>(Arrays.asList(set_punct_values));
		
		for (String punct : punctuation) {
			if (wordCount.containsKey(punct)) {
				size--;
			}
		}
		
		return size;
	}
	
	public static <K,V extends Comparable<? super V>> List<Entry<K, V>> entriesSortedByValues(Map<K,V> results) {
		
		List<Entry<K,V>> sortedEntries = new ArrayList<Entry<K,V>>(results.entrySet());
		
		Collections.sort(sortedEntries, 
		    new Comparator<Entry<K,V>>() {
		        @Override
		        public int compare(Entry<K,V> e1, Entry<K,V> e2) {
		        	//Ascending order
		        	//return e1.getValue().compareTo(e2.getValue());
		        	//Descending order
		            return e2.getValue().compareTo(e1.getValue());
		        }
		    }
		);
		
		return sortedEntries;
	}
	
	/**
	 * 
	 * @param results
	 * @return the toString of the HashMap ordered by value
	 */
	public static String orderHashMapByValueToString(HashMap<String, Float> results) {
	    
	    List<Entry<String, Float>> sortedMap = entriesSortedByValues(results);
	    
	    String result = "";
	    for(Entry<String, Float> entry : sortedMap) {
	    	result += entry.getKey() + ", " + entry.getValue() + "\n";
	    }
	    return result;
	    
	}
	
	/**
	 * 
	 * @param wordCount HashMap containing the word count, (word, number of times used)
	 * @param numWords number of total words used
	 * @return HashMap(String, Float) as (word, frequency of that word)
	 */
	public static HashMap<String, Float> getWordFrequencies(HashMap<String, Integer> wordCount, int numWords){
		
		HashMap<String, Float> wordFreq = new HashMap<String, Float>();
		
		for(Entry<String, Integer> entry : wordCount.entrySet()) {
			wordFreq.put(entry.getKey(), (float) ((float)entry.getValue() / (float)numWords));
		}
		
		return wordFreq;
		
	}

	/**
	 * 
	 * @param knownTwoGrams
	 * @param unkTwoGrams
	 * @return how much the two HashMap have similar entries, in percentage
	 */
	public static float getTwoGramsRatio(HashMap<TextPair, Integer> knownTwoGrams,
			HashMap<TextPair, Integer> unkTwoGrams) {
		
		float result = (float) 0.0;
		
		for(Entry<TextPair, Integer> entry : knownTwoGrams.entrySet()) {
			if(unkTwoGrams.containsKey(entry.getKey())) {
				result += getFloatRatio(unkTwoGrams.get(entry.getKey()), knownTwoGrams.get(entry.getKey()));
			}
			/*
			else {
				add 0;
			}
			*/
		}
		
		return (result / (float) knownTwoGrams.size());
	}

	/**
	 * 
	 * @param knownThreeGrams
	 * @param unkThreeGrams
	 * @return how much the two HashMap have similar entries, in percentage
	 */
	public static float getThreeGramsRatio(HashMap<TextTrigram, Integer> knownThreeGrams,
			HashMap<TextTrigram, Integer> unkThreeGrams) {
		
		float result = (float) 0.0;
		
		for(Entry<TextTrigram, Integer> entry : knownThreeGrams.entrySet()) {
			if(unkThreeGrams.containsKey(entry.getKey())) {
				result += getFloatRatio(unkThreeGrams.get(entry.getKey()), knownThreeGrams.get(entry.getKey()));
			}
			/*
			else {
				add 0;
			}
			*/
		}
		
		return (result / (float) knownThreeGrams.size());
		
	}
	
	/**
	 * 
	 * @param knownWordFreq
	 * @param unkWordFreq
	 * @return how much the two HashMap have similar entries, in percentage
	 */
	public static float getWordFreqRatio(HashMap<String, Float> knownWordFreq, 
			HashMap<String, Float> unkWordFreq) {
		
		float result = (float) 0.0;
		
		for(Entry<String, Float> entry : knownWordFreq.entrySet()) {
			if(unkWordFreq.containsKey(entry.getKey())) {
				result += getFloatRatio(unkWordFreq.get(entry.getKey()), knownWordFreq.get(entry.getKey()));
			}
			/*
			else {
				add 0;
			}
			*/
		}
		
		return (result / knownWordFreq.size());
		
	}

	/**
	 * 
	 * @param knownWordFreq
	 * @param unkWordFreq
	 * @return how much subset of the two HashMap have similar entries, in percentage
	 */
	public static float getWordFreqRatioFromAll(HashMap<String, Float> knownWordFreq, 
			HashMap<String, Float> unkWordFreq) {
		//Extract from 20 to 60 most used words of known author,
		//and confronting them with the 20-60 most used words of unknown author.
		//If authKnown/authUnk has less than 100 entries, I use them all.
		if(knownWordFreq.size() >= 100 && unkWordFreq.size() >= 100) {
			int from = 20;
			int to = 60;
			return getWordFreqRatio(
					extractSubSetOrdered(knownWordFreq, from, to), extractSubSetOrdered(unkWordFreq, from, to));
			
		}
		else {
			return getWordFreqRatio(knownWordFreq, unkWordFreq);
		}
		
	}
	
	/**
	 * 
	 * @param hashMap from which extract subset
	 * @param from number of element from which extract
	 * @param to number of element to which extract
	 * @return return a subset of that HashMap
	 */
	public static HashMap<String, Float> extractSubSetOrdered(HashMap<String, Float> hashMap, 
			int from, int to){
		//extracting 20th-120th entries from ordered set
		//or 0-all if number of entries are less or equal than 150
		
		if(hashMap.size() <= 150) {
			
			List<Entry<String, Float>> sortedEntries = entriesSortedByValues(hashMap);
			List<Entry<String, Float>> subSetOrdered = sortedEntries.subList(0, sortedEntries.size());
			
			HashMap<String, Float> results = new HashMap<String, Float>(128, 0.8f);
			
			for (Entry<String, Float> entry : subSetOrdered) {
				results.put(entry.getKey(), entry.getValue());
			}
			
			return results;
			
		}
		else {
			
			List<Entry<String, Float>> sortedEntries = entriesSortedByValues(hashMap);
			List<Entry<String, Float>> subSetOrdered = sortedEntries.subList(from, to);
			
			HashMap<String, Float> results = new HashMap<String, Float>(128, 0.8f);
			
			for (Entry<String, Float> entry : subSetOrdered) {
				results.put(entry.getKey(), entry.getValue());
			}
			
			return results;
			
		}
		
	}
	
	/**
	 * 
	 * @param hashMap from which extract subset
	 * @param from number of element from which extract
	 * @param to number of element to which extract
	 * @return return a subset of that HashMap
	 */
	public static HashMap<TextPair, Integer> extractSubSetOrderedTextPair(HashMap<TextPair, Integer> hashMap, 
			int from, int to){
		//extracting 1st-100th entries from ordered set
		
		if(hashMap.size() <= 100) {
			
			List<Entry<TextPair, Integer>> sortedEntries = entriesSortedByValues(hashMap);
			List<Entry<TextPair, Integer>> subSetOrdered = sortedEntries.subList(0, sortedEntries.size());
			
			HashMap<TextPair, Integer> results = new HashMap<TextPair, Integer>(128, 0.8f);
			
			for (Entry<TextPair, Integer> entry : subSetOrdered) {
				results.put(entry.getKey(), entry.getValue());
			}
			
			return results;
			
		}
		else {
			
			List<Entry<TextPair, Integer>> sortedEntries = entriesSortedByValues(hashMap);
			List<Entry<TextPair, Integer>> subSetOrdered = sortedEntries.subList(from, to);
			
			HashMap<TextPair, Integer> results = new HashMap<TextPair, Integer>(128, 0.8f);
			
			for (Entry<TextPair, Integer> entry : subSetOrdered) {
				results.put(entry.getKey(), entry.getValue());
			}
			
			return results;
			
		}
		
	}
	
	/**
	 * 
	 * @param hashMap from which extract subset
	 * @param from number of element from which extract
	 * @param to number of element to which extract
	 * @return return a subset of that HashMap
	 */
	public static HashMap<TextTrigram, Integer> extractSubSetOrderedTrigram(HashMap<TextTrigram, Integer> hashMap, 
			int from, int to){
		//extracting 20th-60th entries from ordered set
		//or 0-100 if number of entries are less than 100

		if(hashMap.size() <= 100) {
			
			List<Entry<TextTrigram, Integer>> sortedEntries = entriesSortedByValues(hashMap);
			List<Entry<TextTrigram, Integer>> subSetOrdered = sortedEntries.subList(0, sortedEntries.size());
			
			HashMap<TextTrigram, Integer> results = new HashMap<TextTrigram, Integer>(128, 0.8f);
			
			for (Entry<TextTrigram, Integer> entry : subSetOrdered) {
				results.put(entry.getKey(), entry.getValue());
			}
			
			return results;
			
		}
		else {
			
			List<Entry<TextTrigram, Integer>> sortedEntries = entriesSortedByValues(hashMap);
			List<Entry<TextTrigram, Integer>> subSetOrdered = sortedEntries.subList(from, to);
			
			HashMap<TextTrigram, Integer> results = new HashMap<TextTrigram, Integer>(128, 0.8f);
			
			for (Entry<TextTrigram, Integer> entry : subSetOrdered) {
				results.put(entry.getKey(), entry.getValue());
			}
			
			return results;
			
		}
		
	}

} //end class
