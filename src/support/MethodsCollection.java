package support;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class MethodsCollection {

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

	public static boolean puntuactionChecker(String word) {
		
		String[] set_conj_values = new String[] {	".", ",", ":", ";",
													"?", "!", "(", ")",
													"-", "\""};
		Set<String> conjuction = new HashSet<>(Arrays.asList(set_conj_values));
		
		if(conjuction.contains(word)) {
			return true;
		}
		else {
			return false;
		}
	}

}
