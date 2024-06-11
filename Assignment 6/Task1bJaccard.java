package ex.ex06;

import java.util.HashSet;
import java.util.Set;

public class Task1bJaccard extends Task1bSkeleton
{
	static double jaccard(Set<String> d1, Set<String> d2)
	{
		Set<String> intersection=new HashSet<>();
		intersection.addAll(d1);
		intersection.retainAll(d2);
		
		Set<String> union=new HashSet<>();
		union.addAll(d1);
		union.addAll(d2);

		// alternative without computing the union explicitly:
		// d1.size()+d2.size()-intersection.size()
		
		return ((double)intersection.size())/union.size();
	}
	
	private final static int SHINGLE_SIZES[]= {1,3,5};
	
	public static void main(String args[])
	{
		// iterate over all shingle sizes
		for (int s=0;s<SHINGLE_SIZES.length;s++)
		{
			Shingler shingler=new Task1aWordShingling(SHINGLE_SIZES[s]);
			
			// compute and store shingle sets for each document
			@SuppressWarnings("unchecked")
			Set<String> shingles[]=new Set[documents.length];
			
			for (int c=0;c<documents.length;c++)
			{
				shingles[c]=shingler.computeShingles(documents[c]);
//				System.out.println((shingles[c]));
			}

			System.out.println(SHINGLE_SIZES[s]+"-Shingles:");
			
			// for each pair of documents, compute and output their shingle-based similarity
			for (int c1=0;c1<documents.length-1;c1++)
				for (int c2=c1+1;c2<documents.length;c2++)
				{
					System.out.println("sim("+c1+","+c2+")="+jaccard(shingles[c1],shingles[c2]));
				}
		}
	}

}
