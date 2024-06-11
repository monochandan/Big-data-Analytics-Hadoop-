package assignment_04_02;

public class recursive_test {
	    public static void main(String[] args) {
	        int[] a = {1,2,3,4,5};
	        int i = 0;

	        fun_array(a,i);
	    }

	    private static int fun_array(int[] a, int i) {

	        int length = a.length;
	        if( i >= length)
	        {
	            return 0;
	        }
	        else
	        {
	            System.out.println(a[i]);
	            i = i + 1 ;
	            return fun_array(a, i);
	        }
	    }
	    

}
