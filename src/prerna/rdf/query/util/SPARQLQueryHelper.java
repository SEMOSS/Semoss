/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.rdf.query.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public abstract class SPARQLQueryHelper {
	
	public static String createComponentString(TriplePart triplePart)
	{
		String retString="";
		if(triplePart.getType().equals(TriplePart.VARIABLE))
		{
			retString= createVariableString(triplePart);
		}
		else if(triplePart.getType().equals(TriplePart.URI))
		{
			retString= createURIString(triplePart);
		}
		else if(triplePart.getType().equals(TriplePart.LITERAL))
		{
			retString= createLiteralString(triplePart);
		}
		return retString;
	}

	public static String createVariableString(TriplePart triplePart)
	{
		if(!triplePart.getType().equals(TriplePart.VARIABLE))
		{
			throw new IllegalArgumentException("TriplePart is not set as a variable");
		}
		return "?"+triplePart.getValue();
	}
	
	public static String createURIString(TriplePart triplePart)
	{
		if(!triplePart.getType().equals(TriplePart.URI))
		{
			throw new IllegalArgumentException("TriplePart is not set as a URI");
		}
		return "<"+triplePart.getValue()+">";
	}
	
	public static String createLiteralString(TriplePart triplePart)
	{
		if(!triplePart.getType().equals(TriplePart.LITERAL))
		{
			throw new IllegalArgumentException("TriplePart is not set as a Literal");
		}
		if(triplePart.getValue() instanceof String)
			// if getting back a raw literal value, do not add quotes around or if the quotes are already in place (aka it is a raw string)
			if(triplePart.getValue().toString().contains("http://www.w3.org/2001") || (triplePart.getValue().toString().startsWith("\"") && triplePart.getValue().toString().endsWith("\""))) {
				return triplePart.getValue().toString();
			} else {
				return "'"+triplePart.getValue()+"'";
			}
		else if(triplePart.getValue() instanceof Double)
			//looks something like "1.0"^^<http://www.w3.org/2001/XMLSchema#double>
			return "\"" +((Double)triplePart.getValue()).toString() + "\""+"^^<"+SPARQLConstants.LIT_DOUBLE_URI+">";
		else if(triplePart.getValue() instanceof Integer)
			//looks something like "1"^^http://www.w3.org/2001/XMLSchema#integer
			return "\"" +((Integer)triplePart.getValue()).toString() + "\""+"^^<"+SPARQLConstants.LIT_INTEGER_URI+">";
		else if(triplePart.getValue() instanceof Date)
		{
			Date value = (Date)triplePart.getValue();
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
			String date = df.format(value);
			//looks something like "1"^^http://www.w3.org/2001/XMLSchema#integer
			return "\"" +date + "\""+"^^<"+SPARQLConstants.LIT_DATE_URI+">";
		}

		return "'"+triplePart.getValue()+"'";
	}
}
