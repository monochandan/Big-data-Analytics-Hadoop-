package ex.ex06;

import java.util.Random;

// implementation of a generic hash function
// see slide 4-76

public class GenericHashFunction 
{
	public final static int DEFAULT_P=99991;
	
	long a,b,p,N;
	public GenericHashFunction(int a, int b, int p, int N)
	{
		this.a=a;
		this.b=b;
		this.p=p;
		this.N=N;
	}
	
	// convenience method that takes a shingle as a parameter
	public int hash(String shingle)
	{
		return hash(shingle.hashCode());
	}
	
	public int hash(long x)
	{
		// we are repairing negative input values
		if (x<0) x+=-((long)Integer.MIN_VALUE);
		
		return (int)(((a*x+b)% p)% N);
	}

	// generate array with k randomly chosen hash functions
	// note that this will not work with more than 99991 shingles
	// N is the number of shingles
	public static GenericHashFunction[] generate(int k, int N)
	{
		Random random=new Random(42);
		
		GenericHashFunction h[]=new GenericHashFunction[k];
		
		for (int i=0;i<k;i++)
		{
			h[i]=new GenericHashFunction(random.nextInt(Integer.MAX_VALUE),
										 random.nextInt(Integer.MAX_VALUE),
										 DEFAULT_P,
										 N);
		}
		
		return h;

	}
	public static void main(String args[])
	{
		GenericHashFunction h=new GenericHashFunction(125,727,1001,100);
		
		System.out.println(h.hash("i am a test input".hashCode()));
		System.out.println(h.hash("i am another test input".hashCode()));
	}
}
