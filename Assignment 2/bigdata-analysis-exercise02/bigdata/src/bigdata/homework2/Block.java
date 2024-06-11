package bigdata.homework2;

import java.util.HashSet;
import java.util.Set;

/**
 * Stores the data related to a block
 *
 */
public class Block {

	/** the name used to reference this block (just convenience) */
	private final String label;
	
	/** who is listed in this block  */
	private Set<Name> members = new HashSet<>();
	
	/**
	 * generate the block
	 * @param label
	 * 	the name the block is known by (e.g., 'Hans')
	 */
	public Block(String label) {
		this.label = label;
	}
	
	
	/**
	 * Returns the label (i.e., 'name' of the block) 
	 * @return
	 * 	the label of the block
	 */
	public String getLabel() {
		return this.label;
	}
	
	/**
	 * get the size of the block
	 * @return
	 * 	returns the number of members (i.e., names assigned to this block)
	 */
	public long getSize() {
		return this.members.size();
	}
	
	/**
	 * Add a name to the block
	 */
	public void addMember(Name name) {
		this.members.add(name);
	}
	
	/**
	 * Returns a set of all members of this block.
	 * @return
	 * 	all members of this block. Will return empty set if the block has not been filled yet.
	 */
	public Set<Name> getMembers() {
		return this.members;
	}
	
	/**
	 * Check if name is listed in the Block
	 */
	public boolean hasName(Name name) {
		return this.members.contains(name);
	}
	
	/**
	 * Convenience function to print all members on screen
	 */
	public void listMembers() {
		for(Name name : this.members) {
			System.out.println(name);
		}
	}
	
	/**
	 * For testing
	 */
	public static void main(String[] args) {
		// testing
		
		Block block = new Block("Hans");
		block.addMember(new Name("Hans Müller"));
		block.addMember(new Name("Hans Muller"));

		System.out.println(block.getSize());
	
		block.listMembers();
		
	}

}
