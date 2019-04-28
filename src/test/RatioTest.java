package test;

public class RatioTest {

	public static void main(String[] args) {

		float a = (float) 100.0; //fisso
		float b = (float) 10.0;
		//System.out.println("First method (" + a + ", " + b + ") " + firstMethod(a, b));
		//System.out.println("Second method (" + a + ", " + b + ") " + secondMethod(a, b));
		//System.out.println("Third method (" + a + ", " + b + ") " + thirdMethod(a, b));
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 10;
		b = 100;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 1000;
		b = 10;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 10;
		b = 1000;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 1;
		b = 10000;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 10000;
		b = 1;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 50;
		b = 7;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 7;
		b = 50;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 7;
		b = 5;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 5;
		b = 7;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 10;
		b = 0;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 0;
		b = 10;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 0;
		b = 100;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 0;
		b = 1000;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 10;
		b = 0;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 100;
		b = 0;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 1000;
		b = 0;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 0;
		b = 7;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 12;
		b = 12;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 0;
		b = 0;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 0;
		b = 1;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = (float) 0.1;
		b = 1;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 1000;
		b = 999;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		a = 100;
		b = (float) 99.9;
		System.out.println("Punteggio differenza metodo Elisa (" + a + ", " + b + "): " + metodoElisa(a, b) + "%");
		System.out.print("Punteggio differenza metodo Elisa (" + a + ", " + b + "): ");
        float res = metodoElisa(a, b);
		System.out.printf("%.6f", res);
        System.out.println();
		/*
		0.001
		Punteggio differenza metodo Elisa (0.0, 10.0): 99.99%
		Punteggio differenza metodo Elisa (0.0, 100.0): 99.999%
		Punteggio differenza metodo Elisa (0.0, 1000.0): 99.9999%
		0.01
		Punteggio differenza metodo Elisa (0.0, 10.0): 99.9%
		Punteggio differenza metodo Elisa (0.0, 100.0): 99.99%
		Punteggio differenza metodo Elisa (0.0, 1000.0): 99.999%
		0.0001
		Punteggio differenza metodo Elisa (10.0, 0.0): 99.999%
		Punteggio differenza metodo Elisa (100.0, 0.0): 99.9999%
		Punteggio differenza metodo Elisa (1000.0, 0.0): 99.99999%
		*/
	}
	
	public static float metodoElisa(float a, float b) {
		
		/*
		a e b;
		se a o b è 0, prende lo 0.01% dell'altro valore;
		se sono entrambi 0 ritorno 100(%).
		Se a > b, a è il max val;
		se b > a, b è il max val.
		*/
		
		//dovrebbe ritornare quanto simili sono i due valori
		
		float maxVal, ratioToCalc, x;
		
		if(a == b) { //copre lo 0
			return 1000;
		}
		
		else { //a e b sono sempre positivi
			assert a >= 0;
			assert b >= 0;
			
			if (a > b) {
				if (b == 0) {
					//0 diventa epsilon
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
					//0 diventa epsilon
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
	
	public static float firstMethod(float unkFirst, float knownSecond) {
		
		float difference, percentageDifference, percentageDiffAbs;
		//return 0 when are equal
		if(knownSecond == 0) {
			if(unkFirst == 0) {
				return 0;
			} 
			else {
				knownSecond = unkFirst / 100;
				
				difference = knownSecond - unkFirst;
				
				percentageDifference = difference / knownSecond * 100;
				
				percentageDiffAbs = Math.abs(percentageDifference);
				
				return percentageDiffAbs;
			}
		}
		else {
			
			difference = knownSecond - unkFirst;
			
			percentageDifference = difference / knownSecond * 100;
			
			percentageDiffAbs = Math.abs(percentageDifference);
			
			return percentageDiffAbs;
		}
		
	}
	
	public static float secondMethod(float unkFirst, float knownSecond) {
		
		float average, ratio, ratioAbsPercentage;
		
		if(knownSecond == 0 && unkFirst == 0) {
				return 0;
		}
		else {
			
			average = (unkFirst + knownSecond) / 2;
			ratio = (unkFirst - knownSecond) / average;
			ratioAbsPercentage = 100 * Math.abs(ratio);
			return ratioAbsPercentage;
			
		}
		
	}
	
	public static float thirdMethod(float unkFirst, float knownSecond) {
		
		float gcd = gcd(unkFirst, knownSecond);
		if (unkFirst > knownSecond) {
			return unkFirst/gcd;
		}
		else {
			return knownSecond/gcd;
		}
		   
	}
	
	public static float gcd(float a, float b) {
	    if (b == 0) return a;
	    else return gcd(b, a % b);
	}

	public void ratio(int a, int b) {
	   final float gcd = gcd(a,b);
	   System.out.println(a/gcd + " " + b/gcd);
	}
	public static void showAnswer(float f, float g) {
		   System.out.println(f + " " + g);
	}
}
