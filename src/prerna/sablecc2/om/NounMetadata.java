package prerna.sablecc2.om;

import java.util.Hashtable;

public class NounMetadata extends Hashtable 
{
	public enum QUANTITY {SINGLE, MULTIPLE};
	public enum REQUIRED{ TRUE, FALSE};
	
	public NounMetadata()
	{
		super.put("QUANTITY", QUANTITY.SINGLE);
		super.put("EXPLAINATION", "You need to over ride your explanation");
		super.put("REQUIRED", REQUIRED.TRUE);
		super.put("NOUN", "s");
	}
	
}
