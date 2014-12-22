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

import java.util.Date;

public class TriplePart {

	TriplePartConstant type;
	Object value;
	public final static TriplePartConstant VARIABLE = new TriplePartConstant("VARIABLE");
	public final static TriplePartConstant URI = new TriplePartConstant("URI");
	public final static TriplePartConstant LITERAL = new TriplePartConstant("LITERAL");
	
	public TriplePart (Object value, TriplePartConstant type)
	{	
		if (type == TriplePart.VARIABLE || type == TriplePart.URI || type == TriplePart.LITERAL)
		{
			this.type = type;
			this.value = value;
		}
		if (!(value instanceof String) && (type ==TriplePart.VARIABLE || type ==TriplePart.URI))
		{
			throw new IllegalArgumentException("Non-String values cannot be used as a variable part or URI part");
		}
		if (!(value instanceof String) && !(value instanceof Integer) && !(value instanceof Double)&& !(value instanceof Date))
		{
			throw new IllegalArgumentException("Value can only be String, Integer, Double or Date at this moment");
		}
	}
	
	public Object getValue()
	{
		return value;
	}
	
	public TriplePartConstant getType()
	{
		return type;
	}
	
	public String getTypeString()
	{
		return type.getConstant();
	}
	
}
