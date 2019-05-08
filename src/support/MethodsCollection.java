package support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.FloatWritable;

/**
 * Collection of methods used for Author Attribution
 * @author Paolo D'Arienzo
 *
 */
public class MethodsCollection {

	/**
	 * 
	 * @param word
	 * @return true if word is a punctuation symbol, false otherwise
	 */
	public static boolean punctuationChecker(String word) {
		
		String[] set_punct_values = new String[] {	".", ",", ":", ";",
													"?", "!", "(", ")",
													"-", "\""};
		Set<String> punctuation = new HashSet<>(Arrays.asList(set_punct_values));
		
		if(punctuation.contains(word)) {
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
		
		String[] set_function_words = new String[] 
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
		
		Set<String> function_words = new HashSet<>(Arrays.asList(set_function_words));

		if(function_words.contains(word)) {
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
	public static long getTotalCharsOnWords(HashMap<String, Integer> wordVal) {
		
		long totalChars = 0;
				
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
	public static long getTotalWords(HashMap<String, Integer> wordVal) {
		//counts punctuation too
		long totalWords = 0;
		
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
	public static long getTotalWordsWOPunct(HashMap<String, Integer> wordVal) {
		//excludes punctuation
		long totalWords = 0;
		
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
		double numTotalWords = (double) getTotalWordsWOPunct(wordCount);
		
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
	
	/**
	 * 
	 * @param results
	 * @return the HashMap ordered by descending value
	 */
public static LinkedHashMap<String, Float> orderHashMapByValue(HashMap<String, Float> results) {
		
		List<String> mapKeys = new ArrayList<>(results.keySet());
	    List<Float> mapValues = new ArrayList<>(results.values());
	    Collections.sort(mapValues, Collections.reverseOrder());
	    Collections.sort(mapKeys, Collections.reverseOrder());
	    //for ascending order
	    //Collections.sort(mapValues);
	    //Collections.sort(mapKeys);

	    LinkedHashMap<String, Float> sortedMap = new LinkedHashMap<>();

	    Iterator<Float> valueIt = mapValues.iterator();
	    while (valueIt.hasNext()) {
	    	Float val = valueIt.next();
	        Iterator<String> keyIt = mapKeys.iterator();

	        while (keyIt.hasNext()) {
	        	String key = keyIt.next();
	        	Float comp1 = results.get(key);
	        	Float comp2 = val;

	            if (comp1.equals(comp2)) {
	                keyIt.remove();
	                sortedMap.put(key, val);
	                break;
	            }
	        }
	    }
	    
	    return sortedMap;
	    
	}

	public static LinkedHashMap<TextPair, Integer> orderHashMapByValueTextPair(HashMap<TextPair, Integer> results) {
	
	List<TextPair> mapKeys = new ArrayList<>(results.keySet());
    List<Integer> mapValues = new ArrayList<>(results.values());
    Collections.sort(mapValues, Collections.reverseOrder());
    Collections.sort(mapKeys, Collections.reverseOrder());
    //for ascending order
    //Collections.sort(mapValues);
    //Collections.sort(mapKeys);

    LinkedHashMap<TextPair, Integer> sortedMap = new LinkedHashMap<>();

    Iterator<Integer> valueIt = mapValues.iterator();
    while (valueIt.hasNext()) {
    	Integer val = valueIt.next();
        Iterator<TextPair> keyIt = mapKeys.iterator();

        while (keyIt.hasNext()) {
        	TextPair key = keyIt.next();
        	Integer comp1 = results.get(key);
        	Integer comp2 = val;

            if (comp1.equals(comp2)) {
                keyIt.remove();
                sortedMap.put(key, val);
                break;
            }
        }
    }
    
    return sortedMap;
    
}

	public static LinkedHashMap<TextTrigram, Integer> orderHashMapByValueTrigram(HashMap<TextTrigram, Integer> results) {
	
	List<TextTrigram> mapKeys = new ArrayList<>(results.keySet());
    List<Integer> mapValues = new ArrayList<>(results.values());
    Collections.sort(mapValues, Collections.reverseOrder());
    Collections.sort(mapKeys, Collections.reverseOrder());
    //for ascending order
    //Collections.sort(mapValues);
    //Collections.sort(mapKeys);

    LinkedHashMap<TextTrigram, Integer> sortedMap = new LinkedHashMap<>();

    Iterator<Integer> valueIt = mapValues.iterator();
    while (valueIt.hasNext()) {
    	Integer val = valueIt.next();
        Iterator<TextTrigram> keyIt = mapKeys.iterator();

        while (keyIt.hasNext()) {
        	TextTrigram key = keyIt.next();
        	Integer comp1 = results.get(key);
        	Integer comp2 = val;

            if (comp1.equals(comp2)) {
                keyIt.remove();
                sortedMap.put(key, val);
                break;
            }
        }
    }
    
    return sortedMap;
    
}
	
	/**
	 * 
	 * @param results
	 * @return the string of the HashMap ordered by value
	 */
	public static String orderHashMapByValueToString(HashMap<String, Float> results) {
	    
	    LinkedHashMap<String, Float> sortedMap = orderHashMapByValue(results);
	    
	    String result = "";
	    for(Entry<String, Float> entry : sortedMap.entrySet()) {
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
	public static HashMap<String, Float> getWordFrequencies(HashMap<String, Integer> wordCount, long numWords){
		
		HashMap<String, Float> wordFreq = new HashMap<String, Float>();
		
		for(Entry<String, Integer> entry : wordCount.entrySet()) {
			wordFreq.put(entry.getKey(), (float) ((double)entry.getValue() / (double)numWords));
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
		//estraggo tra le 20 e le 60 parole più usate dell'autore noto,
		//e le confronto con le 20-60 parole più usate dell'autore ignoto.
		//Se l'authKnown/authUnk ha meno di 100 entries, le uso tutte.
		
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
	 * @param hashMap
	 * @param from
	 * @param to
	 * @return return a subset of that HashMap
	 */
	public static LinkedHashMap<String, Float> extractSubSetOrdered(HashMap<String, Float> hashMap, 
			int from, int to){
		//extracting 20th-60th entries from ordered set
		LinkedHashMap<String, Float> orderedMap = orderHashMapByValue(hashMap);
		LinkedHashMap<String, Float> subSetOrdered = new LinkedHashMap<String, Float>();
		
		if(hashMap.size() < 100) {
			int counter = 0;
			for(Entry<String, Float> entry : orderedMap.entrySet()) {
				counter++;
				if(counter < 0) {
					continue;
				}
				else if(counter > 100) {
					break;
				}
				else {
					subSetOrdered.put(entry.getKey(), entry.getValue());
				}
			}
			return subSetOrdered;
		}
		else {
			int counter = 0;
			for(Entry<String, Float> entry : orderedMap.entrySet()) {
				counter++;
				if(counter < from) {
					continue;
				}
				else if(counter > to) {
					break;
				}
				else {
					subSetOrdered.put(entry.getKey(), entry.getValue());
				}
			}
			return subSetOrdered;
		}
	}
	
	public static LinkedHashMap<TextPair, Integer> extractSubSetOrderedTextPair(HashMap<TextPair, Integer> hashMap, 
			int from, int to){
		//extracting 20th-60th entries from ordered set
		LinkedHashMap<TextPair, Integer> orderedMap = orderHashMapByValueTextPair(hashMap);
		LinkedHashMap<TextPair, Integer> subSetOrdered = new LinkedHashMap<TextPair, Integer>();
		
		if(hashMap.size() < 100) {
			int counter = 0;
			for(Entry<TextPair, Integer> entry : orderedMap.entrySet()) {
				counter++;
				if(counter < 0) {
					continue;
				}
				else if(counter > 100) {
					break;
				}
				else {
					subSetOrdered.put(entry.getKey(), entry.getValue());
				}
			}
			return subSetOrdered;
		}
		else {
			int counter = 0;
			for(Entry<TextPair, Integer> entry : orderedMap.entrySet()) {
				counter++;
				if(counter < from) {
					continue;
				}
				else if(counter > to) {
					break;
				}
				else {
					subSetOrdered.put(entry.getKey(), entry.getValue());
				}
			}
			return subSetOrdered;
		}
	}
	
	public static LinkedHashMap<TextTrigram, Integer> extractSubSetOrderedTrigram(HashMap<TextTrigram, Integer> hashMap, 
			int from, int to){
		//extracting 20th-60th entries from ordered set
		LinkedHashMap<TextTrigram, Integer> orderedMap = orderHashMapByValueTrigram(hashMap);
		LinkedHashMap<TextTrigram, Integer> subSetOrdered = new LinkedHashMap<TextTrigram, Integer>();
		
		if(hashMap.size() < 100) {
			int counter = 0;
			for(Entry<TextTrigram, Integer> entry : orderedMap.entrySet()) {
				counter++;
				if(counter < 0) {
					continue;
				}
				else if(counter > 100) {
					break;
				}
				else {
					subSetOrdered.put(entry.getKey(), entry.getValue());
				}
			}
			return subSetOrdered;
		}
		else {
			int counter = 0;
			for(Entry<TextTrigram, Integer> entry : orderedMap.entrySet()) {
				counter++;
				if(counter < from) {
					continue;
				}
				else if(counter > to) {
					break;
				}
				else {
					subSetOrdered.put(entry.getKey(), entry.getValue());
				}
			}
			return subSetOrdered;
		}
	}

} //end class
