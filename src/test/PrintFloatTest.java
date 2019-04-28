package test;

public class PrintFloatTest {

	public static void main(String[] args) {
		
		String value = "7.6666666E-4";
		float floatVal = Float.valueOf(value);
		System.out.println(floatVal);
		System.out.printf("%.6f", floatVal);
		System.out.println();
		
		String statsToString = "";
		String numberAsString = String.format("%6f", floatVal);
		statsToString += "Average word length ratio: " + numberAsString + "\n";
		System.out.println(statsToString);
		
	}

}
