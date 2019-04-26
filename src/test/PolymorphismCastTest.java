package test;

public class PolymorphismCastTest {

	public static void main(String[] args) {
		
		int first, second;
		first = 5;
		second = 10;
		
		System.out.println(checkCastStatic(first, second));
		
		DinObj dinObj = new DinObj();
		
		System.out.println(dinObj.checkCastDynamic(first, second));
		
	}
	
	public static float checkCastStatic(float first, float second) {
		return first / second; //0.5
	}
	/*
	public static float checkCastStatic(float first, int second) {
		return (float) 0.10;
	}
	public static float checkCastStatic(int first, float second) {
		return (float) 0.20;
	}
	public static float checkCastStatic(int first, int second) {
		return (float) 0.30;
	}
	*/
	
}

class DinObj {
	
	int first;
	int second;

	public DinObj() {
		
	}
	
	public float checkCastDynamic(float first, float second) {
		return first / second; //0.5
	}
	
}
