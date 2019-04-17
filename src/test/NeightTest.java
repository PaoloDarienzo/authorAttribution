package test;

import java.util.ArrayList;
import java.util.List;

public class NeightTest {

	public static void main(String[] args) {
		
		List<String> words = new ArrayList<String>();
		String prec, prePrec, post, postPost;
		int indexTerm;
		
		words.add("ciao");
		words.add("come");
		words.add("va");
		words.add("ciao");
		words.add("carciofo");
		words.add("ciao");
		words.add("carciofo");
		words.add("uovo");
		words.add("in");
		words.add("camicia");
		words.add("ultimo");
		
		System.out.println(words.indexOf("ultimo"));
		
		System.out.println(words.size() + ", " + words.toString());
		
		indexTerm = -1;
		for(String term : words) {
			post = ""; postPost = "";
			indexTerm++;
			if(!(indexTerm + 2 >= words.size())) {
				//if postPost exists, post exists
				postPost = words.get(indexTerm + 2);
				post = words.get(indexTerm + 1);
			}
			else if(!(indexTerm + 1 >= words.size())) {
				post = words.get(indexTerm + 1);
			}
			if(!postPost.isEmpty()) {
				//if postPost exists, post exists
				System.out.println("term: " + term + ", post: " + post + ", postPost: " + postPost);
				System.out.println("term: " + term + ", post: " + post);
			}
			else if(!post.isEmpty()) {
				System.out.println("term: " + term + ", post: " + post);
			}
		}
		
		System.out.println("#######################");
		
		indexTerm = -1;
		for(String term : words) {
			prec = ""; prePrec = "";
			post = ""; postPost = "";
			indexTerm++;
			if(!(indexTerm - 2 < 0)) {
				prePrec = words.get(indexTerm - 2);
			}
			if(!(indexTerm - 1 < 0)) {
				prec = words.get(indexTerm - 1);
			}
			if(!(indexTerm + 2 >= words.size())) {
				postPost = words.get(indexTerm + 2);
			}
			if(!(indexTerm + 1 >= words.size())) {
				post = words.get(indexTerm + 1);
			}
			if(!prePrec.isEmpty()) {
				System.out.println("prePrec: " + prePrec + ", term: " + term);
			}
			if(!prec.isEmpty()) {
				System.out.println("prec: " + prec + ", term: " + term);
			}
			if(!post.isEmpty()) {
				System.out.println("post: " + post + ", term: " + term);
			}
			if(!postPost.isEmpty()) {
				System.out.println("postPost: " + postPost + ", term: " + term);
			}
		}
		
		System.out.println("#######################");
		indexTerm = -1;
		for(String term : words) {
			prec = "";
			post = "";
			indexTerm++;
			if(!(indexTerm - 1 < 0)) {
				prec = words.get(indexTerm - 1);
			}
			if(!(indexTerm + 1 >= words.size())) {
				post = words.get(indexTerm + 1);
			}
			if(!prec.isEmpty()) {
				System.out.println("prec: " + prec + ", term: " + term);
			}
			if(!post.isEmpty()) {
				System.out.println("post: " + post + ", term: " + term);
			}
		}
		
	}

}
