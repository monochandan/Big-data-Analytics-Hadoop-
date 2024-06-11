package ex.ex06;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class Task1aCharacterShingling extends AbstractShingler
{
	public Task1aCharacterShingling(int k)
	{
		super(k);
	}

	public String normalize(String text)
	{
		return text.replace('\"', ' ')
				  .replace('!', ' ')
//				  .replace('\'', ' ')
				  .replace('?', ' ')
				  .replace('(', ' ')
				  .replace(')', ' ')
//				  .replace('-', ' ')
				  .replace('.', ' ')
				  .replace(':', ' ')
				  .replace(';', ' ')
				  .replace('=', ' ')
				  .replace(',',' ')
				  .replace('[',' ')
				  .replace(']',' ')
				  .replace('“',' ')
				  .replace('”',' ')
				  .toLowerCase()
				  .trim();
	}
	
	@Override
	public Set<String> computeShingles(String text, int k)
	{
		char tokens[]=normalize(text).toCharArray();
		
		if (tokens.length<k) return Collections.emptySet();
		
		Set<String> shingles=new HashSet<>();
		
		for (int i=0;i<=tokens.length-k;i++)
		{
			shingles.add(new String(Arrays.copyOfRange(tokens, i, i+k)));
		}
		
		return shingles;
	}
	
	private final static String[] cases= {
			"Data Science is an upcoming field that combines methods from computer science, mathematics and statistics.",
			"Data Science is an emerging field that brings together methods from statistics, mathematics, and computing."
		};

	private static final int[] SHINGLE_SIZES= {3};
	
	public static void main(String args[])
	{
		for (int s=0;s<SHINGLE_SIZES.length;s++)
		{
			System.out.println("k="+SHINGLE_SIZES[s]);
			Shingler shingler=new Task1aCharacterShingling(SHINGLE_SIZES[s]);
			
			@SuppressWarnings("unchecked")
			Set<String> shingles[]=new Set[cases.length];
			
			for (int c=0;c<cases.length;c++)
			{
				shingles[c]=shingler.computeShingles(cases[c]);
				System.out.println((shingles[c]));
			}
		}
	}
}
