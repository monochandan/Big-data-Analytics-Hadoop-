package ex.ex06;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Task1cMinHashing extends Task1bSkeleton
{
	private GenericHashFunction[] h;

	int numberHashFunctions=5; // default
	int shingleSize=5; // default
	
	// construct Shingler with default config
	Shingler shingler=new Task1aWordShingling(shingleSize);
	
	public Task1cMinHashing(int k)
	{
		numberHashFunctions=k;
	}

	public Task1cMinHashing(int k, int n)
	{
		this(k);
		shingleSize=n;
		shingler=new Task1aWordShingling(n);
	}

	/*
	 * compute minHash signatures of the input documents
	 * 
	 * @param documents: array with input documents as Strings
	 * 
	 * @returns matrix with document signatures
	 */
	public int[][] computeSignatures(String[] documents)
	{
		@SuppressWarnings("unchecked")
		Set<String> shingles[]=new Set[documents.length];
		
		for (int c=0;c<documents.length;c++)
		{
			shingles[c]=shingler.computeShingles(documents[c],5);
			System.out.println((shingles[c]));
		}

		return computeSignatures(shingles);
	}

	/*
	 * compute minHash signatures of the input documents
	 * 
	 * @param documents: array with shingle sets of the documents
	 * 
	 * @returns matrix with document signatures
	 */
	public int[][] computeSignatures(Set<String>[] documents)
	{
		// first: compute overall number of shingles (N)
		Set<String> shingles=new HashSet<>();
		
		for (Set<String> s: documents)
			shingles.addAll(s);
		
		int N=shingles.size();
		
//		System.out.println(N+" different shingles.");

		// next: create random hash functions using GenericHashFunction's factory method
		h=GenericHashFunction.generate(numberHashFunctions, N);
		
		// array to store document signature matrix
		int[][] minHash=new int[documents.length][];
		
		// compute signature for each document using the fast algorithm
		for (int i=0;i<documents.length;i++)
			minHash[i]=computeSignature(documents[i]);
		
		return minHash;
	}
	
	private int[] computeSignature(Set<String> document)
	{
		int[] signature=new int[h.length];
		Arrays.fill(signature,Integer.MAX_VALUE); // set initial value

		// now compute, for each shingle in the document,
		// its hash values and compare them to the current minimum in the corresponding dimension
		
		for (String shingle: document)
		{
			for (int f=0;f<h.length;f++)
			{
				int v=h[f].hash(shingle);
				if (v<signature[f])
				{	
//					System.out.println("signature["+f+"] set to "+v+" based on \""+shingle+"\", was "+signature[f]+" before");
					signature[f]=v;
				}
			}
		}
		return signature;
	}

	// compute minhash signature for a single document
	// note that this requires that the number of shingles (or at least an upper bound) is known
	public int[] computeSignature(String document, int N)
	{
		Set<String> shingles=shingler.computeShingles(document);
		
		// create random hash functions using GenericHashFunction's factory method
		h=GenericHashFunction.generate(numberHashFunctions, N);
		
		return computeSignature(shingles);
	}

	static double overlap(int[] sig1, int[] sig2)
	{
		if (sig1.length!=sig2.length) return 0.0;
		
		int common=0;
		for (int i=0;i<sig1.length;i++)
			if (sig1[i]==sig2[i]) common=common+1;
		return ((double)common)/sig1.length;
	}
	
	private final static int SHINGLE_SIZES[]= {5};
	
	private final static int NUM_HASH_FUNCTIONS[] = {1,5,10};
	
	public static void main(String args[])
	{
		// consider all shingle sizes
		for (int s=0;s<SHINGLE_SIZES.length;s++)
		{
			Shingler shingler=new Task1aWordShingling(SHINGLE_SIZES[s]);

			// for each document, compute and store its shingles
			@SuppressWarnings("unchecked")
			Set<String> shingles[]=new Set[documents.length];
			
			for (int c=0;c<documents.length;c++)
			{
				shingles[c]=shingler.computeShingles(documents[c]);
				System.out.println((shingles[c]));
			}

			// for all given numbers of hash functions, compute the minhash signatures
			for (int k=0;k<NUM_HASH_FUNCTIONS.length;k++)
			{			
				System.out.println("\n"+NUM_HASH_FUNCTIONS[k]+" hash functions, "+SHINGLE_SIZES[s]+"-Shingles");
				
				// construct a MinHash implementation for the given number of hash functions and shingle size
				Task1cMinHashing minHasher=new Task1cMinHashing(NUM_HASH_FUNCTIONS[k],SHINGLE_SIZES[s]);

				// compute minhash signatures for all documents represented by their shingles
				// note that shingles[] is an array of shingle sets
				int[][] minhash=minHasher.computeSignatures(shingles);
				
				// output minhash signatures for each document
				for (int c=0;c<minhash.length;c++)
				{
					System.out.println("signature["+c+"]="+Arrays.toString(minhash[c]));
				}
				
				// for each document pair, output its similarity based on the minhash signatures
				for (int c1=0;c1<documents.length-1;c1++)
					for (int c2=c1+1;c2<documents.length;c2++)
					{
						if (overlap(minhash[c1],minhash[c2])>0.2) System.out.println("sigsim("+c1+","+c2+")="+overlap(minhash[c1],minhash[c2]));
					}
			}
		}
	}

}
