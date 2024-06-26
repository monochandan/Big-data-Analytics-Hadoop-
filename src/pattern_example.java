import java.util.regex.Pattern;

public class pattern_example {
	public static void main(String[] args) {
	      String date = "12/09/2019";
	      String regex = "^(1[0-2]|0[1-9])/(3[01]|[12][0-9]|0[1-9])/[0-9]{4}$";
	      //Creating a pattern object
	      Pattern pattern = Pattern.compile(regex);
	      if(pattern.matcher(date).matches()) {
	         System.out.println("Date is valid");
	      } else {
	         System.out.println("Date is not valid");
	      }
	      //Retrieving the regular expression of the current pattern
	      String regularExpression = pattern.pattern();
	      System.out.println("Regular expression: "+regularExpression);
	   }

}
