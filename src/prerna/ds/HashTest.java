package prerna.ds;

import java.util.Hashtable;

public class HashTest {
public static void main(String [] args)
{
	Hashtable <Integer, Integer> trial = new Hashtable();
	for(int i =0;i < 9000000;i++)
	{
		trial.put(i, i);
	}

	System.out.println("Done");
}


	
}
