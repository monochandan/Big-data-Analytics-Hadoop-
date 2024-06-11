package ex.ex06;

import java.util.Set;

public abstract class AbstractShingler implements Shingler 
{
	int shingleSize=5; // default
	
	public AbstractShingler(int k)
	{
		shingleSize=k;
	}
	
	@Override
	public Set<String> computeShingles(String text)
	{
		return computeShingles(text,shingleSize);
	}
}
