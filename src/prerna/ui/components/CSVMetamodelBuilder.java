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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

public class CSVMetamodelBuilder {

	public List<String> getHeaders(ArrayList<File> files){
		// get the headers for one CSVFile
		Iterator<File> it = files.iterator();
		ArrayList<String> outputHeaders = new ArrayList<String>();
		while(it.hasNext())
		{
			String[] header = null;
			try {
				ICsvMapReader mapReader = new CsvMapReader(new FileReader(it.next()), CsvPreference.STANDARD_PREFERENCE);
				header = mapReader.getHeader(true);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		

			// need to check if last header in CSV file is an absolute path to a prop file
			File propFile = new File(header[header.length-1]);
			if(propFile != null){
				header = Arrays.copyOfRange(header, 0, header.length-1);
			}
			
			for(int i = 0; i < header.length; i++){
				outputHeaders.add(header[i]);
			}
		}
		return outputHeaders;
	}
}
