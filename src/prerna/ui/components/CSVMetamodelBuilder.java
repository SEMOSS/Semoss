/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.components;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

public class CSVMetamodelBuilder {

	private ArrayList<File> files;
	private Hashtable<String, Hashtable<String, LinkedHashSet<String>>> dataType = new Hashtable<String, Hashtable<String, LinkedHashSet<String>>>();
	private String[] header;

	public void setFiles(ArrayList<File> files) {
		this.files = files;
	}

	public Hashtable<String, Hashtable<String, LinkedHashSet<String>>> returnDataTypes()
	{
		//TODO: loop through multiple files?
		
		boolean successful = true;
		File fileName = files.get(0);
		CsvListReader listReader = null;
		try {
			listReader = new CsvListReader(new FileReader(fileName), CsvPreference.STANDARD_PREFERENCE);
			this.header = listReader.getHeader(true);
		}		
		catch (FileNotFoundException e) {
			successful = false;
			e.printStackTrace();
		} catch (IOException e) {
			successful = false;
			e.printStackTrace();
		}
		
		if(successful) {
			initiateDataTypeHash();
			getAllDataType(listReader);
			getAllowedDataType();
			return dataType;
		}
		
		return dataType;
	}

	private void initiateDataTypeHash() {
		LinkedHashSet<String> headerSet = new LinkedHashSet<String>();
		for(int i = 0; i < header.length; i++)
		{
			headerSet.add(header[i]);
			if(header[i] != null) 
			{
				Hashtable<String, LinkedHashSet<String>> innerHash = new Hashtable<String, LinkedHashSet<String>>();
				innerHash.put("AllDataTypes", new LinkedHashSet<String>());
				innerHash.put("AllowedDataTypes", new LinkedHashSet<String>());
				dataType.put(header[i], innerHash);
			}
		}
		
		Hashtable<String, LinkedHashSet<String>> headerHash = new Hashtable<String, LinkedHashSet<String>>();
		headerHash.put("AllHeaders", headerSet);
		dataType.put("AllHeaders", headerHash);

	}

	private void getAllDataType(CsvListReader listReader)
	{
		List<String> instances;
		try {
			while((instances = listReader.read()) != null)
			{
				for(int i = 0; i < header.length; i++)
				{
					if(instances.get(i) != null)
					{
						String type = new String(determineProcessor(instances.get(i)));
						dataType.get(header[i]).get("AllDataTypes").add(type);
						dataType.get(header[i]).get("AllowedDataTypes").add(type);
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	//This method determines what data types are available in the dropdown for each property
	private void getAllowedDataType()
	{
		for(int i = 0; i < header.length; i++)
		{
			if(header[i] != null){
				Hashtable<String, LinkedHashSet<String>> headerObj = dataType.get(header[i]);
				LinkedHashSet<String> allowedTypes = headerObj.get("AllowedDataTypes");
	
				if(allowedTypes.isEmpty()) // it should never come in here, but as default, String is the only thing available
				{
					allowedTypes.add("STRING");
				}
				else if(allowedTypes.size() == 1) // if all cells of that column are the same type, you can load as that specific type, or string
				{
					allowedTypes.add("STRING");
				}
				else if(allowedTypes.size() >= 2) // if cell of that column are two different types
				{
					allowedTypes.clear();
					allowedTypes.add("STRING");
				}
			}

//			HashSet<String> numSet = new HashSet<String>();
//			numSet.add("DOUBLE");
//			numSet.add("INTEGER");
//			if(allowedTypes.isEmpty()) // it should never come in here, but as default, String is the only thing available
//			{
//				allowedTypes.add("STRING");
//			}
//			else if(allowedTypes.size() == 1) // if all cells of that column are the same type, you can load as that specific type, or string
//			{
//				allowedTypes.add("STRING");
//			}
//			else if(allowedTypes.size() == 2) // if cell of that column are two different types
//			{
//				if(allowedTypes.equals(numSet)) // if the two options are double and int, int is not an option as doubles cannot be cast to int
//				{
//					allowedTypes.remove("INTEGER");
//					allowedTypes.add("STRING");
//				}
//				else // otherwise only string is available as anything else would have cast exception
//				{
//					allowedTypes.clear();
//					allowedTypes.add("STRING");
//				}
//			}
//			else if(allowedTypes.size() > 2) // if more than two types, only string is available
//			{
//				Iterator<String> typeIt = allowedTypes.iterator();
//				while(typeIt.hasNext())
//				{
//					typeIt.next();
//					typeIt.remove();
//				}
//				allowedTypes.add("STRING");
//			}
		}
	}

	public static String determineProcessor(String s) {
		
//		boolean isInt = true;
//		try { 
//			Integer.parseInt(s); 
//		} catch(NumberFormatException e) { 
//			isInt = false;
//		}
//
//		if(isInt){
//			return ("INTEGER");
//		}

		boolean isDouble = true;
		try {
			Double.parseDouble(s);
		} catch(NumberFormatException e) {
			isDouble = false;
		}

		if(isDouble) {
			return ("DOUBLE");
		}

		//TODO: combine determining long date vs. simple date into a loop

		Boolean isLongDate = true;
		SimpleDateFormat formatLongDate = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
		Date longdate = null;
		try {
			formatLongDate.setLenient(true);
			longdate  = formatLongDate.parse(s);
		} catch (ParseException e) {
			isLongDate = false;
		}

		if(isLongDate){
			return ("DATE");
		}

		Boolean isSimpleDate = true;
		SimpleDateFormat formatSimpleDate = new SimpleDateFormat("mm/dd/yyyy");
		Date simpleDate = null;
		try {
			formatSimpleDate.setLenient(true);
			simpleDate  = formatSimpleDate.parse(s);
		} catch (ParseException e) {
			isSimpleDate = false;
		}

		if(isSimpleDate){
			return ("SIMPLEDATE");
		}

		if(Boolean.parseBoolean(s)){
			return ("BOOLEAN");
		}

		return ("STRING");
	}
}
