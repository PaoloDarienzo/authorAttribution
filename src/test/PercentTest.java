package test;

public class PercentTest {

	public static void main(String[] args) {

		float avgWordLenFact = (float) 1; //			5%
		float funDensFact = (float) 3; // 				40%
		float punctDensFact = (float)  2; //				20%
		float ttrFact = (float) 1.5; //					20%
		float wordFreqFact = (float) 1; //				5%
		float twoGramsFact = (float) 1; //				5%
		float threeGramsFact = (float) 1; //			5%
		float sum = avgWordLenFact+ funDensFact+punctDensFact+
				ttrFact+wordFreqFact+twoGramsFact+threeGramsFact;
		float numFactOne = 10;
		float numFact = (float) sum;
		float result = 0;
		
		result = (float) (96.731216*avgWordLenFact + 
				99.461647*funDensFact + 
				88.163078*punctDensFact + 
				95.908096*ttrFact + 
				38.248363*wordFreqFact + 
				21.068840*twoGramsFact + 
				16.768253*threeGramsFact);
		
		System.out.println(result / numFactOne);
		System.out.println(result / numFact);
		
	}

}
