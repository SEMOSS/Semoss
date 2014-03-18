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
package prerna.poi.specific;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;

import prerna.util.Constants;
import prerna.util.DIHelper;

//TODO: this class is never used

/**
 * Replaces system names with dummy names
 */
public class TAPDataScrambler{

	Hashtable<String,String> priorKeyHash = new Hashtable<String, String>();	
	Hashtable<String,String> keyHash = new Hashtable<String, String>();	
	Hashtable<String,String> typeHash = new Hashtable<String, String>();

	public String concat = "";

	/**
	 * Constructor for TAPDataScrambler.
	 */
	public TAPDataScrambler()
	{
		priorKeyHash = getScramblerProperties("/DashedSystemScramblerProp.properties");
		typeHash = getScramblerProperties("/TypeProp.properties");
		keyHash = getScramblerProperties("/SystemScramblerProp.properties");
	}

	/**
	 * Method processName.
	 * @param curString 	String 
	 * @param curType		String
	 * @return retString	String 
	 */
	public String processName(String curString, String curType)
	{
		String retString = null;
		if (typeHash.get(curType) == null)
		{
			retString = curString;
		}
		else if (typeHash.get(curType).equals("-"))
		{
			this.concat = "-";
			retString = processCurString(curString);
		}
		else if (typeHash.get(curType).equals("%"))
		{
			this.concat = "%";
			retString = processCurString(curString);
		}
		return retString;
	}

	//TODO: this method returns a property and not a hashtable?
	/**
	 * Method getScramblerProperties.
	 * @param fileName String
	 * @return Hashtable 
	 */
	public Hashtable getScramblerProperties(String fileName){

		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String propFile = workingDir + fileName;
		Properties scrambleProperties = null;
		try {
			scrambleProperties = new Properties();
			scrambleProperties.load(new FileInputStream(propFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return scrambleProperties;
	}


	/**
	 * Method processTypeOne.
	 * @param curString String
	 * @return String 
	 */
	public String processCurString(String curString)
	{
		Iterator<String> it =  priorKeyHash.keySet().iterator();
		while (it.hasNext())
		{
			String key = (String) it.next();
			if (curString.contains(this.concat+key) || curString.contains(key+this.concat) || curString.equals(key))
			{
				curString = curString.replace(key, priorKeyHash.get(key));
			}
		}
		it =  keyHash.keySet().iterator();
		while (it.hasNext())
		{
			String key = (String) it.next();
			if (curString.contains(this.concat+key) || curString.contains(key+this.concat) || curString.equals(key))
			{
				curString = curString.replace(key, keyHash.get(key));
			}
		}
		return curString;
	}
}
