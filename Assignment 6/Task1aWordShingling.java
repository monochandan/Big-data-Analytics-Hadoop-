package ex.ex06;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class Task1aWordShingling extends AbstractShingler
{
	final static String SHINGLE_SEPARATOR=" ";

	public Task1aWordShingling(int k)
	{
		super(k);
	}

	private String normalize(String text)
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
		String tokens[]=normalize(text).split("[\\s]+");
		
		if (tokens.length<k) return Collections.emptySet();
		
		Set<String> shingles=new HashSet<>();
		
		for (int i=0;i<=tokens.length-k;i++)
		{
			String shingle="";
			for (int p=0;p<k;p++)
			{
				if (p>0) shingle+=SHINGLE_SEPARATOR;
				shingle+=tokens[i+p];
			}
			shingles.add(shingle);
		}
		
		return shingles;
	}

	private final static String[] cases= {
			"Data Science is an upcoming field that combines methods from computer science, mathematics and statistics.",
			"Data Science is an emerging field that brings together methods from statistics, mathematics, and computing."
		};

	private static final int[] SHINGLE_SIZES= {3};
	
	public static void main(String args[])
	{
		for (int s=0;s<SHINGLE_SIZES.length;s++)
		{
			System.out.println("k="+SHINGLE_SIZES[s]);
			Shingler shingler=new Task1aWordShingling(SHINGLE_SIZES[s]);
			
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
