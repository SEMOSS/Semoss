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
			Hashtable<String, LinkedHashSet<String>> innerHash = new Hashtable<String, LinkedHashSet<String>>();
			innerHash.put("AllDataTypes", new LinkedHashSet<String>());
			innerHash.put("AllowedDataTypes", new LinkedHashSet<String>());
			dataType.put(header[i], innerHash);
		}
		
		Hashtable<String, LinkedHashSet<String>> headerHash = new Hashtable<String, LinkedHashSet<String>>();
		headerHash.put("AllHeaders", headerSet);
		dataType.put("AllHeaders", headerHash);

	}

	private void getAllDataType(CsvListReader listReader)
	{
		List<String> instances;
		try {
			instances = listReader.read();
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

	private void getAllowedDataType()
	{
		HashSet<String> numSet = new HashSet<String>();
		numSet.add("DOUBLE");
		numSet.add("INTEGER");

		for(int i = 0; i < header.length; i++)
		{
			if(dataType.get(header[i]).get("AllowedDataTypes").isEmpty())
			{
				dataType.get(header[i]).get("AllowedDataTypes").add("STRING");
			}
			else if(dataType.get(header[i]).get("AllowedDataTypes").size() == 1)
			{
				dataType.get(header[i]).get("AllowedDataTypes").add("STRING");
			}
			else if(dataType.get(header[i]).get("AllowedDataTypes").size() == 2)
			{
				if(dataType.get(header[i]).get("AllowedDataTypes").equals(numSet))
				{
					dataType.get(header[i]).get("AllowedDataTypes").remove("INTEGER");
					dataType.get(header[i]).get("AllowedDataTypes").add("STRING");
				}
				else
				{
					dataType.get(header[i]).get("AllowedDataTypes").clear();
					dataType.get(header[i]).get("AllowedDataTypes").add("STRING");
				}
			}
			else if(dataType.get(header[i]).get("AllowedDataTypes").size() > 2)
			{
				Iterator<String> typeIt = dataType.get(header[i]).get("AllowedDataTypes").iterator();
				while(typeIt.hasNext())
				{
					typeIt.next();
					typeIt.remove();
				}
				dataType.get(header[i]).get("AllowedDataTypes").add("STRING");
			}
		}
	}

	public static String determineProcessor(String s) {
		String processor = "";

		boolean isInt = true;
		try { 
			Integer.parseInt(s); 
		} catch(NumberFormatException e) { 
			isInt = false;
		}

		if(isInt){
			return (processor = "INTEGER");
		}

		boolean isDouble = true;
		try {
			Double.parseDouble(s);
		} catch(NumberFormatException e) {
			isDouble = false;
		}

		if(isDouble) {
			return (processor = "DOUBLE");
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
			return (processor = "DATE");
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
			return (processor = "SIMPLEDATE");
		}

		if(Boolean.parseBoolean(s)){
			return (processor = "BOOLEAN");
		}

		return (processor = "STRING");
	}
}
