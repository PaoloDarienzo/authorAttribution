package test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import support.MethodsCollection;

public class RegexTest {
	
	private final static Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
	private final static Pattern WORD_BOUNDARY_FOR_SYMBOLS = Pattern.compile("\\s*\\s*");
	public static final String[] SET_PUNCT_VALUES = new String[] {	".", ",", ":", ";",
																	"?", "!", "(", ")",
																	"-", "\""};
	public static final Set<String> PUNCTUATION = new HashSet<>(Arrays.asList(SET_PUNCT_VALUES));
	
	  public static void main(String[] args) {

		  String line = "(Ciao, come va?) Ho spazi e punti. \"E apici!\"; altri simboli random utilizzati" + 
		  " sono: !?()\\ detto questo... fine. Ah, provo!?bo.";
		  
		  for (String word : WORD_BOUNDARY.split(line)) {
						
			  if (word.isEmpty()) {
					continue; //skipping to next legitimate word
			  }
			  
			  else if((!(word.matches("^[a-zA-Z0-9]*$")) && !(PUNCTUATION.contains(word)))) {
				  for(String wordSymb : WORD_BOUNDARY_FOR_SYMBOLS.split(word)) {
					  if (wordSymb.isEmpty()) {
						  continue; //skipping to next legitimate word
					  }
					  //System.out.println(wordSymb);  
				  }
			  }
			  else {
		      	//System.out.println(word);
			  }
		    
		    }
		  
		  boolean isWord;
		  ArrayList<String> currentBookTrace = new ArrayList<String>();
		  List<String> words = new ArrayList<String>();
		  int punctNo = 0;
		  int funcNo = 0;
		  
		  for(String word: WORD_BOUNDARY.split(line)) {
				//now the line is splitted in words;
				//punctuation symbols that are near, i.e. "!?", are not divided yet.
				
				if (word.isEmpty()) {
					//skip if word is empty
					continue;
				}
				else if ((!(word.matches("^[a-zA-Z0-9]*$")) && !(PUNCTUATION.contains(word)))) {
					//skip if word is not a digit or a char
					//and is not a punctuation symbol
					//NB: set of punctuation are not checked yet
					for(String wordSymb : WORD_BOUNDARY_FOR_SYMBOLS.split(word)) {
						//now the word is splitted in singular punctuation symbols;
						//only punctuation will be accepted, else continue.
						if (wordSymb.isEmpty()
								||
								!(PUNCTUATION.contains(wordSymb))
							){
							continue; //skipping to next legitimate word
						}
						else {
							if(MethodsCollection.punctuationChecker(wordSymb)) {
								punctNo++;
							}
							//wordCount
							currentBookTrace.add(wordSymb);
						}
						
					  } //end cycle over set of punctuation
					
				}	//end if over not words or not numbers or not singular punctuation symbols
					//or block of singular punctuation symbols
				//only words with digit or chars get here;
				//not symbols, nor punctuation
				else {
					isWord = true;
					if(MethodsCollection.punctuationChecker(word)) {
						punctNo++;
						isWord = false;
					}
					else if(MethodsCollection.functionWordChecker(word)) {
						funcNo++;
					}
					String lowCaseWord = word.toLowerCase();
					//wordCount
					currentBookTrace.add(lowCaseWord);
					if(isWord) {
						//twoGrams/threeGrams with only words
						words.add(lowCaseWord);
					}
				}
				
			} //end for
		  
		  System.out.println(currentBookTrace.toString());
		  System.out.println(punctNo + ", " + funcNo);
		  
	  }
}
