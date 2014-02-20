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
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.supercsv.io.CsvListReader;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

public class CSVMetamodelBuilder {

	private ArrayList<File> files;
	private Hashtable<String, Hashtable<String, Set<String>>> dataType = new Hashtable<String, Hashtable<String, Set<String>>>();
	private String[] header;

	public void setFiles(ArrayList<File> files) {
		this.files = files;
	}

	public Hashtable<String, Hashtable<String, Set<String>>> returnDataTypes(){

		//TODO: loop through multiple files?

		File fileName = files.get(0);

		CsvListReader listReader = null;
		String[] header = null;
		try {
			listReader = new CsvListReader(new FileReader(fileName), CsvPreference.STANDARD_PREFERENCE);
			this.header = listReader.getHeader(true);
		}		
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		initiateDataTypeHash();
		getAllDataType(listReader);
		getAllowedDataType();

		return this.dataType;
	}

	private void initiateDataTypeHash() {
		Hashtable<String, Set<String>> innerHash = new Hashtable<String, Set<String>>();

		innerHash.put("AllDataTypes", new HashSet<String>());
		innerHash.put("AllowedDataTypes", new HashSet<String>());


		for(int i = 0; i < header.length; i++)
		{
			this.dataType.put(header[i], innerHash);
		}

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
					String dataType = determineProcessor(instances.get(i));
					this.dataType.get(header[i]).get("AllDataTypes").add(dataType);
					this.dataType.get(header[i]).get("AllowedDataTypes").add(dataType);
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
			if(this.dataType.get(header[i]).get("AllowedDataTypes").isEmpty())
			{
				this.dataType.get(header[i]).get("AllowedDataTypes").add("STRING");
			}
			else if(this.dataType.get(header[i]).get("AllowedDataTypes").size() == 1)
			{
				this.dataType.get(header[i]).get("AllowedDataTypes").add("STRING");
			}
			else if(this.dataType.get(header[i]).get("AllowedDataTypes").size() == 2)
			{
				if(this.dataType.get(header[i]).get("AllowedDataTypes").equals(numSet))
				{
					this.dataType.get(header[i]).get("AllowedDataTypes").remove("INTEGER");
					this.dataType.get(header[i]).get("AllowedDataTypes").add("STRING");
				}
				else
				{
					this.dataType.get(header[i]).get("AllowedDataTypes").clear();
					this.dataType.get(header[i]).get("AllowedDataTypes").add("STRING");
				}
			}
			else if(this.dataType.get(header[i]).get("AllowedDataTypes").size() > 2)
			{
				Iterator<String> typeIt = this.dataType.get(header[i]).get("AllowedDataTypes").iterator();
				while(typeIt.hasNext())
				{
					typeIt.next();
					typeIt.remove();
				}
				this.dataType.get(header[i]).get("AllowedDataTypes").add("STRING");
			}
		}
	}

	public static String determineProcessor(String s) {
		String processor = "";

		// if column is left blank
		if(s == null){
			return (processor = "STRING");
		}

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
