package prerna.engine.impl.web;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
			String[] headers = getHeaders(url, prop);
			Integer[] selectedIndex = null;
			String[] selectedHeaders = null;
			// add filters
			if (query != null) {
				// specified in query so only send specific columns
				List<String> headersList = Arrays.asList(headers);
				String[] selectCols = query.split(";");
				selectedIndex = new Integer[selectCols.length];
				selectedHeaders = new String[selectCols.length];
				// get indexes of columns the user has selected
				for (int i = 0; i < selectCols.length; i++) {
					String[] headerSplit = selectCols[i].split("=");
					System.out.println(Arrays.asList(headerSplit));
					String header = headerSplit[0].trim();
					selectedIndex[i] = headersList.indexOf(header);
					selectedHeaders[i] = header;
				}
			} else {
				throw new IllegalArgumentException("Must select one or more values from table headers!");
			}
			List<String[]> values = collectValues(thisTable.getElementsByTag("tr"), headers, Arrays.asList(selectedIndex));


			// add filtered headers if they exist
			if (selectedHeaders != null) {
				retHash.put("HEADERS", selectedHeaders);
			} else {
				retHash.put("HEADERS", headers);
			}
			String [] types = getWebTypes(values);
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
	
	
	
	
	public String [] getHeaders(String url, Map aliasMap)
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
			// see if there are rows
			Elements allRows = thisTable.getElementsByTag("tr");
			if(allRows != null && allRows.size() > 0)
			{
				Elements firstRow = allRows.get(0).getElementsByTag("th");
				headers = collectHeaders(firstRow);
			}
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

	public List collectValues(Elements values, String [] headerList, List<Integer> selectedHeaderIndicies)
	{
		int rowIndex = 0;
		
		// need to get the td inside of each tr
		// and then paint it
		// I also need to account for table spans
		
		// along with row span.. need to accomodate for column spans as well
		// basically we fill the same value for each of the pieces
		
		
		List <String []> valueList = new Vector();

		// start from 1 since the first one is the header
		Hashtable rowColValue = new Hashtable();
		Hashtable rowSpanCount = new Hashtable();

		for(rowIndex = 1;rowIndex < values.size();rowIndex++)
		{
			Element thisRow = values.get(rowIndex);
//			System.out.println("Processing row.. " + thisRow);
			// get the TD on this row
			// some of these are TH still.. thank you so much
			Elements cols = thisRow.children();
			
			// get the attribute to see 
			// I need to keep the index
			// and I need to keep for how much I need to do that
			String [] oneRow = new String[selectedHeaderIndicies.size()];
			int colIndex = 0;
			//int colspancount = -1;
			//String colSpanData = null;
			boolean dataFilled = false;
			for(Integer headerIndex : selectedHeaderIndicies)
			{
//				colIndex = selectedHeaderIndicies
				if(rowColValue.containsKey(headerIndex))
				{
					// need to use this value for the header
					oneRow[headerIndex] = (String)rowColValue.get(headerIndex);
					int numrows = (Integer)rowSpanCount.get(headerIndex);
					numrows--;
					if(numrows == 0)
					{
						rowColValue.remove(headerIndex);
						rowSpanCount.remove(headerIndex);
					}
					else
						rowSpanCount.put(headerIndex, numrows);
					// move the header forward
					//headerIndex++;
				}
				/*if(headerIndex < colspancount)
				{
					// when a actual data comes need to see what is coming
					oneRow[headerIndex] = colSpanData;
				}*/
				else if(cols.size() > headerIndex)
				{
					dataFilled = true;
					Element thisCol = cols.get(headerIndex);
					oneRow[colIndex] = thisCol.text();
					colIndex++;
					
					// get the attribute to see if it a row span
					String rowspan = thisCol.attr("rowspan");
					if(rowspan != null && rowspan.length() > 0)
					{
						int numrows = Integer.parseInt(rowspan);
						// remove for this row we just saw
						numrows--;
						rowColValue.put(headerIndex, thisCol.text());
						rowSpanCount.put(headerIndex, numrows);
					}
					
					// get the attribute to see if it a row span
					/*String colspan = thisCol.attr("colspan");
					if(colspan != null && colspan.length() > 0)
					{
						colspancount = Integer.parseInt(colspan);
					}*/

				}
			}
			if(dataFilled)
				valueList.add(oneRow);				
		}
/*		while(rowIndex < values.size())
		{
			String [] oneRow = new String[headerList.length];
			for(int headerIndex = 0;headerIndex < headerList.length;headerIndex++)
			{
				oneRow[headerIndex] =  values.get(rowIndex).text();
				rowIndex++;
			}
			valueList.add(oneRow);
		}
*/		//valueList.set(0, headerList);
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
