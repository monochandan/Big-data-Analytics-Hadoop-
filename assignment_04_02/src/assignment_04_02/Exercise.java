package assignment_04_02;

import java.util.Scanner;

public class Exercise {
	 public static void main(String[] args) {
	        Scanner sc = new Scanner(System.in);

	        System.out.print("String: ");
	        String string = sc.nextLine();

	        String result = reverse(string);
	        System.out.println("Result: " + result);
	    }

	private static String reverse(String string) {
		// TODO Auto-generated method stub
		
		int len = string.length();
		String revrs_str = "";
		if(len == 0)
			return revrs_str;
	
		else
			revrs_str += string.charAt(len-1);
			len = len - 1;
			string = string.substring(0, len);
			return revrs_str + reverse(string);
		
	}
}
