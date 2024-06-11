package bigdata.homework2;

/**
 * stores a single person name mention
 * 
 * @author freitz
 *
 */
public class Name {

	public final String firstName;
	public final String lastName;
	
	/** The full name as provided when creating the object */
	public final String baseName; 
	
	/**
	 * creates a name object
	 * @param baseName
	 * 	the full name of the person
	 */
	public Name(String baseName) {
		this.baseName = baseName;
		
		String[] tokens = baseName.split(" ");
		
		this.firstName = tokens[0].trim();
		this.lastName = tokens[tokens.length-1].trim();
	}
	
	/**
	 * Convenience function to print the name
	 */
	public String toString() {
		return this.baseName;
	}
	
	/**
	 * For testing
	 */
	public static void main(String[] args) {
		// testing
		
		Name test1 = new Name("Peter M. Jones");
		System.out.println(test1.baseName + "\t" + test1.firstName +"\t" + test1.lastName);
		
		Name test2 = new Name("Sammy");
		System.out.println(test2.baseName + "\t" + test2.firstName +"\t" + test2.lastName);
		
		Name test3 = new Name("Karl-Heinz Schmidt");
		System.out.println(test1.baseName + "\t" + test3.firstName +"\t" + test3.lastName);
	}		
	
}
