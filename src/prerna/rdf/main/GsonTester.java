package prerna.rdf.main;

import java.util.Hashtable;

import com.google.gson.Gson;

public class GsonTester {
	public static void main(String [] args)
	{
		Hashtable hash = new Hashtable();
		hash.put("1", "arg1");
		hash.put("2", "arg1");
		hash.put("3", "arg1");
		hash.put("4", "arg1");
		Hashtable hash2 = new Hashtable();
		hash2.put("1", "arg1");
		hash2.put("2", "arg1");
		hash2.put("3", "arg1");
		hash2.put("4", "arg1");
		hash.put("Hash", hash2);
		
		
		Gson gson = new Gson();
		System.out.println(gson.toJson(hash));
		
	}

}
