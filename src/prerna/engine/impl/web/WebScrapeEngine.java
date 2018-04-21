package prerna.engine.impl.web;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import org.apache.commons.lang3.math.NumberUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import prerna.engine.impl.json.JsonAPIEngine;
import prerna.util.Utility;

public class WebScrapeEngine extends JsonAPIEngine{

	// override execute query and get enginetype etc. 
	
	// gets the URL - Mostly with the parameters
	// specifies if it is get or post
	// gets the stream and loads it into jsoup
	// gets the selector and runs it
	
	// the selector is cardinality (which specific table to pick) from the page
	// table_number = 3
	// or
	// table_id = abcd
	// or 
	// table_class = number = pick it based on class
	// or 
	// table_attribute = number
	public static final String table_number = "table_number";
	public static final String table_id = "table_id";
	public static final String table_class = "table_class";
	
	
	@Override
	public ENGINE_TYPE getEngineType() {
		// TODO Auto-generated method stub
		return prerna.engine.api.IEngine.ENGINE_TYPE.WEB;
	}

	
	
	@Override
	public Object execQuery(String query) 
	{
		Hashtable retHash  = new Hashtable();
		try {
			String url = prop.getProperty(input_url);
			String method = "get";
			if(prop.containsKey(input_method))
				method = prop.getProperty(input_method);
			
			InputStream is = null;
			
			if(method.equalsIgnoreCase("get"))
				is = doGetI(url);
			else
				is = doPostI(new Hashtable());
			
			Document doc = Jsoup.parse(is, "UTF-8", "");
			
			Element thisTable = null;
			
			if(prop.containsKey(table_class))
			{
				// this is a get by class
				String className = prop.getProperty(table_class);
				int tableNum = Integer.parseInt(prop.getProperty(table_number).trim());
				
				Elements allTables = doc.getElementsByClass(className);
				
				thisTable = allTables.get(tableNum);
			}
			else if(prop.containsKey(table_id))
			{
				// get the table by id
				String id = prop.getProperty(table_id);

				thisTable = doc.getElementById(id);
				
			}
			else
			{
				// this is just table by tag
				int tableNum = Integer.parseInt(prop.get(table_number) + "");

				Elements allTables = doc.getElementsByTag("table");
				
				thisTable = allTables.get(tableNum);			
			}
			
			// now I need to collect the header and rows and then return it
			String [] headers = collectHeaders(thisTable.getElementsByTag("th"));
			List <String []> values = collectValues(thisTable.getElementsByTag("td"), headers);
			
			String [] types = getWebTypes(values);

			retHash.put("HEADERS", headers);
			retHash.put("ROWS", values);
			retHash.put("TYPES", types);
			
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retHash;
	}
	
	public String [] getHeaders(String url, HashMap aliasMap)
	{
		String [] headers = null;
		try {
			String method = "get";
			if(aliasMap.containsKey(input_method))
				method = (String)aliasMap.get(input_method);
			
			InputStream is = null;
			
			if(method.equalsIgnoreCase("get"))
				is = doGetI(url);
			else
				is = doPostI(new Hashtable());
			
			Document doc = Jsoup.parse(is, "UTF-8", "");
			
			Element thisTable = null;
			
			if(aliasMap.containsKey(table_class))
			{
				// this is a get by class
				String className = (String)aliasMap.get(table_class);
				int tableNum = Integer.parseInt(aliasMap.get(table_number) + "");
				
				Elements allTables = doc.getElementsByClass(className);
				
				thisTable = allTables.get(tableNum);
			}
			else if(aliasMap.containsKey(table_id))
			{
				// get the table by id
				String id = (String)aliasMap.get(table_id);

				thisTable = doc.getElementById(id);
				
			}
			else
			{
				// this is just table by tag
				int tableNum = Integer.parseInt(aliasMap.get(table_number) + "");

				Elements allTables = doc.getElementsByTag("table");
				
				thisTable = allTables.get(tableNum);			
			}
			
			// now I need to collect the header and rows and then return it
			headers = collectHeaders(thisTable.getElementsByTag("th"));
		}catch (Exception ex)
		{
			ex.printStackTrace();
		}
		
		return headers;
	}

	public int getNumTables(String url, HashMap aliasMap)
	{
		int tables= 0;
		try {
			String method = "get";
			if(aliasMap.containsKey(input_method))
				method = (String)aliasMap.get(input_method);
			
			InputStream is = null;
			
			if(method.equalsIgnoreCase("get"))
				is = doGetI(url);
			else
				is = doPostI(new Hashtable());
			
			Document doc = Jsoup.parse(is, "UTF-8", "");
			
			tables = doc.getElementsByTag("table").size();
		}catch (Exception ex)
		{
			ex.printStackTrace();
		}
		
		return tables;
	}

	
	public String [] collectHeaders(Elements headers)
	{
		String [] headerList = new String[headers.size()];
		
		for(int headerIndex = 0;headerIndex < headers.size();headerIndex++)
			headerList[headerIndex] = Utility.makeAlphaNumeric(headers.get(headerIndex).text());
		
		return headerList;
		
	}

	public List collectValues(Elements values, String [] headerList)
	{
		int rowIndex = 0;
		
		List <String []> valueList = new Vector();
		while(rowIndex < values.size())
		{
			String [] oneRow = new String[headerList.length];
			for(int headerIndex = 0;headerIndex < headerList.length;headerIndex++)
			{
				oneRow[headerIndex] =  values.get(rowIndex).text();
				rowIndex++;
			}
			valueList.add(oneRow);
		}
		//valueList.set(0, headerList);
		return valueList;
		
	}


	public String [] getWebTypes(List <String []> values)
	{
		
		// try it with 10
		// for every column.. I need to see which are the possible ones
		// and then select it
		
		int rowLimit = 10;
		
		if(values.size() <= 10)
			rowLimit = values.size();

		String [] oneRow = values.get(0);

		String [] typeList = new String[oneRow.length];
		
		int [] stringCount = new int[oneRow.length];
		int [] intCount = new int[oneRow.length];
		
		for(int rowIndex = 0;rowIndex <  rowLimit;rowIndex++)
		{
			oneRow = values.get(rowIndex);
			
			for(int colIndex = 0;colIndex < oneRow.length;colIndex++)
			{
				if(rowIndex == 0)
				{
					stringCount[colIndex] = 0;
					intCount[colIndex] = 0;
				}
				
				String thisValue = oneRow[colIndex];
				if(NumberUtils.isDigits(thisValue))
					intCount[colIndex] = intCount[colIndex] + 1;
				else
					stringCount[colIndex] = stringCount[colIndex] + 1;
				
			}
		}
		
		for(int colIndex = 0;colIndex < intCount.length;colIndex++)
		{
			if(stringCount[colIndex] > intCount[colIndex])
				typeList [colIndex] = "String";
			else
				typeList[colIndex] = "int";
			
		}
		
		
		return typeList;
	}
	
	
	
	
}
