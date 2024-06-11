package bigdata.homework2;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

public class Processor {

	/**
	 * reads a csv file and provides the name pairs extracted from it.
	 * 
	 * @param datafile the file that contains the name pairs
	 * @return pairs of names from the same line. If a name should occur more than
	 *         once, it will be stored in a different name object.
	 * @throws FileNotFoundException
	 */
	public static Map<Name, Name> getNamePairs(File datafile) throws FileNotFoundException {

		Map<Name, Name> result = new HashMap<>();

		int excludeLineCounter = 0;

		try (Scanner filescanner = new Scanner(datafile);) {
			while (filescanner.hasNextLine()) {
				String name = filescanner.nextLine();
				String[] names = name.split(",");

				if (names.length != 2) {
					// System.err.println("Found malformated input line " + name);
					excludeLineCounter++;
				} else {
					Name name1 = new Name(names[0].trim());
					Name name2 = new Name(names[1].trim());

					result.put(name1, name2);
				}
			}
		}

		System.out.println(
				"Read name pairs. Found " + result.size() + " pairs and rejected " + excludeLineCounter + " Lines");

		return result;
	}

	/**
	 * Compute blocks with strategy s_1 (first/given name)
	 * 
	 * @param allNames all names extracted from the csv file
	 * @return the computed blocks
	 */
	public static Collection<Block> computeFirstNameBlocks(Set<Name> allNames) {

		Map<String, Block> blocks = new HashMap<>();

		for (Name name : allNames) {
			if (!blocks.containsKey(name.firstName)) {
				blocks.put(name.firstName, new Block(name.firstName));
				// System.out.println("created new block " + name.firstName);
			}

			blocks.get(name.firstName).addMember(name);
		}

		return blocks.values();
	}

	/**
	 * Compute blocks with strategy s_2 (last/family name)
	 * 
	 * @param allNames all names extracted from the csv file
	 * @return the computed blocks
	 */
	public static Collection<Block> computeLastNameBlocks(Set<Name> allNames) {

		Map<String, Block> blocks = new HashMap<>();

		for (Name name : allNames) {
			if (!blocks.containsKey(name.lastName)) {
				blocks.put(name.lastName, new Block(name.lastName));
				// System.out.println("created new block " + name.lastName);
			}

			blocks.get(name.lastName).addMember(name);
		}

		return blocks.values();
	}

	/**
	 * Computes recall 
	 * @param goldstandard
	 * 	the gold standard from the provided test data set
	 * @param blocks
	 * 	the blocks computed and to be evaluated
	 * @return
	 * 	the recall as defined in the exercise 
	 */
	public static double computeRecall(Map<Name, Name> goldstandard, Collection<Block> blocks) {

		Map<Name, Block> nameMapping = new HashMap<>();

		for (Block block : blocks) {
			for (Name name : block.getMembers()) {
				nameMapping.put(name, block);
			}
		}

		int allPairs =0;
		int pairsThatMatch =0;
	
		for (Name name1 : goldstandard.keySet()) {
			Name name2 = goldstandard.get(name1);
			allPairs++;

			Block block1 = nameMapping.get(name1);
			Block block2 = nameMapping.get(name2);

			if(block1.equals(block2)) {
				pairsThatMatch++;
			}
		}

		return pairsThatMatch / (allPairs *1.0);
	}

	/**
	 * compute the saving value
	 * 
	 * @param allNames a set of all names. This is a blocking where all names are
	 *                 placed in the same block
	 * @param blocks   the blocks computed
	 * @return the fraction of comparisons that have been saved with the blocking
	 *         strategy
	 */
	public static double computeSaving(Set<Name> allNames, Collection<Block> blocks) {

		long allNameSize = allNames.size();

		long allComparisons = (allNameSize * (allNameSize - 1)) / 2;

		long blockComparisons = 0;
		for (Block block : blocks) {
			blockComparisons += (block.getSize() * (block.getSize() - 1)) / 2;
		}

		return 1 - (blockComparisons * 1.0 / allComparisons);
	}

	/**
	 * Main method, required the data set file (uncompressed)
	 * @throws FileNotFoundException
	 */
	public static void main(String[] args) throws FileNotFoundException {

		if (args.length != 1) {
			System.err.println("ABORTING   usage:  ... <csv_file.csv>");
		}

		Map<Name, Name> goldStandard = getNamePairs(new File(args[0]));

		Set<Name> allNames = new HashSet<>(goldStandard.keySet());
		allNames.addAll(goldStandard.values());

		Collection<Block> firstNameBlocks = computeFirstNameBlocks(allNames);
		Collection<Block> lastNameBlocks = computeLastNameBlocks(allNames);

		System.out.println("Recall first names: " + computeRecall(goldStandard, firstNameBlocks));
		System.out.println("Recall last names : " + computeRecall(goldStandard, lastNameBlocks));
		
		System.out.println("Saving first names: " + computeSaving(allNames, firstNameBlocks));
		System.out.println("Saving last names : " + computeSaving(allNames, lastNameBlocks));
	}

}
