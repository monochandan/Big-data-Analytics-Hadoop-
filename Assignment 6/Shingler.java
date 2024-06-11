package ex.ex06;

import java.util.Set;

public interface Shingler 
{
	public Set<String> computeShingles(String text, int k);
	
	// use the default shingle length
	public Set<String> computeShingles(String text);

}
