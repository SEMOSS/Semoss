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
