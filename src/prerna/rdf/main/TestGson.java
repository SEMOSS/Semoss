package prerna.rdf.main;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

public class TestGson {

	public static void main(String[] args) {
		Gson gson = new Gson();
		Map<String, Object> test = new HashMap<String, Object>();
		test.put("test1", 2.0);
		test.put("test2", "string");
		
		String stringify = gson.toJson(test);
		System.out.println(stringify);
		Map<String, Object> x = gson.fromJson(stringify, Map.class);
		double number = (double) x.get("test1");
		System.out.println(number);
	}
	
}
