import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class simple_n_gram {

    public static List<String> N_Gram(int nmin, int nmax, String str)
    {
        List<String> ngram = new ArrayList<String>();
        List<String> words = new ArrayList<String>();
        str = str.replace(".", "");
        words = Arrays.asList(str.split(" "));

        System.out.println(words);
        for(int j = nmin; j <= nmax; j++)
        {
            for(int i = 0; i < words.size()-j+1;i++)
            {
                ngram.add(String.valueOf(words.subList(i,i+j)));
            }
        }

        return ngram;
    }
    public static void main(String[] args) {
        String str = "My name is chandan. I live in Trier. I am the student of Data Sciene and studying in Universität Trier. I love this beautiful City. The people of this city are awesome";

        int nmax = 4;
        int nmin = 2;
        List<String> ngrams = N_Gram(nmin, nmax,str);
        //N_Gram(nmin, nmax,str);
        for(String n: ngrams)
        {
            System.out.println(n);
        }
    }
}
