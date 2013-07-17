package prerna.poi.main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;

public class TAPDataScrambler {

	Hashtable<String,String> priorKeyHash = new Hashtable();	
	Hashtable<String,String> keyHash = new Hashtable();	
	Hashtable<String,String> typeHash = new Hashtable();
	public TAPDataScrambler()
	{
		priorKeyHash = getScramblerProperties("/DashedSystemScramblerProp.properties");
		typeHash = getScramblerProperties("/TypeProp.properties");
		keyHash = getScramblerProperties("/SystemScramblerProp.properties");
	}
	
	public String processName (String curString, String curType)
	{
		String retString = null;
		if (typeHash.get(curType)==null)
		{
			retString = curString;
		}
		else if (typeHash.get(curType).equals("-"))
		{
			retString = processTypeOne(curString);
		}
		else if (typeHash.get(curType).equals("%"))
		{
			retString = processTypeTwo(curString);
		}
		return retString;
	}
	
    public Hashtable getScramblerProperties(String fileName){
    	
        String workingDir = System.getProperty("user.dir");
        String propFile = workingDir + fileName;
        Properties scrambleProperties = null;
        try {
               scrambleProperties = new Properties();
               scrambleProperties.load(new FileInputStream(propFile));
        } catch (FileNotFoundException e) {
               // TODO Auto-generated catch block
               e.printStackTrace();
        } catch (IOException e) {
               // TODO Auto-generated catch block
               e.printStackTrace();
        }
        return scrambleProperties;
 }

	
	//IfConcat is with "-"
	public String processTypeOne (String curString)
	{
		Iterator it =  priorKeyHash.keySet().iterator();
		
		while (it.hasNext())
		{
			String key = (String) it.next();
			if (curString.contains("-"+key) || curString.contains(key+"-") || curString.equals(key))
			{
				curString = curString.replace(key, priorKeyHash.get(key));
			}
		}
		
		it =  keyHash.keySet().iterator();
		
		while (it.hasNext())
		{
			String key = (String) it.next();
			if (curString.contains("-"+key) || curString.contains(key+"-") || curString.equals(key))
			{
				curString = curString.replace(key, keyHash.get(key));
			}
		}
		return curString;
	}
	
	//IfConcat is with "%"
	public String processTypeTwo (String curString)
	{
		Iterator it =  priorKeyHash.keySet().iterator();
		
		while (it.hasNext())
		{
			String key = (String) it.next();
			if ((curString.contains("%"+key) || curString.contains(key+"%"))|| curString.equals(key))
			{
				curString = curString.replace(key, priorKeyHash.get(key));
			}
		}
		
		it =  keyHash.keySet().iterator();
		
		while (it.hasNext())
		{
			String key = (String) it.next();
			if (curString.contains("%"+key) || curString.contains(key+"%")|| curString.equals(key))
			{
				curString = curString.replace(key, keyHash.get(key));
			}
		}
		return curString;
	}
}
