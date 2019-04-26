package support;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.FloatWritable;

public class MethodsCollection {

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
	
	public static long getTotalChars(HashMap<String, Integer> wordVal) {
		
		long totalChars = 0;
				
		for (Entry<String, Integer> entry : wordVal.entrySet()) {
		    String key = entry.getKey();
		    Integer value = entry.getValue();
		    totalChars += key.length() * value.intValue();
		}
		
		return totalChars;
		
	}

	public static long getTotalWords(HashMap<String, Integer> wordVal) {
		
		long totalWords = 0;
		
		for (Entry<String, Integer> entry : wordVal.entrySet()) {
		    Integer value = entry.getValue();
		    totalWords += value.intValue();
		}
		
		return totalWords;
		
	}
	
	public static float getFloatRatio(FloatWritable unkFirst, FloatWritable knownSecond) {
		return getFloatRatio(unkFirst.get(), knownSecond.get());
	}
	
	//TODO
	/*
	 * Ricordarsi:
	 * i valori che arrivano sono in percentuale ma da 0 a 1;
	 * sistemare i valori per le size delle strutture;
	 * controllare che func e punt chiamino valori corretti
	 */
	public static float getFloatRatio(float a, float b) {
		/*
		It returns how much two numbers a, b are different, in percentage 
		(0 are the same, 99.99999% very different).
		If a or b are 0, they are substituted to the epsilon value 0.0001.
		If they are both 0, they are equal, so it returns 0.
		*/
		float maxVal, ratioToCalc, x;
		
		if(a == b) { //covers 0 case
			return 0;
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
				return 100 - x;
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
				return 100 - x;
			}
		}
		
	}
	
	public static float getSizeRatio(int unkFirst, int knownSecond) {
		return getFloatRatio((float) unkFirst, (float) knownSecond);
	}

}
