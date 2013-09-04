package prerna.util;

import java.text.NumberFormat;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JOptionPane;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;

import com.ibm.icu.text.DecimalFormat;

public class Utility {
	
	public static int id = 0;

	public static String [] testToken(String uri) {
		StringTokenizer tokens = new StringTokenizer(
				uri, "/");
		int totalTok = tokens.countTokens();
		String [] retString = new String[2];

		for (int tokIndex = 0; tokIndex <= totalTok && tokens.hasMoreElements(); tokIndex++) {
			if (tokIndex + 2 == totalTok)
				retString[0] = tokens.nextToken();
			else if (tokIndex + 1 == totalTok)
				retString[1] = tokens.nextToken();
			else
				tokens.nextToken();

		}
		return retString;
	}
	
	public static Hashtable getParams(String query)
	{	
		Hashtable paramHash = new Hashtable();		
		Pattern pattern = Pattern.compile("[@]{1}\\w+[-]*\\w+@");
		
		Matcher matcher = pattern.matcher(query);
		String test2 = null;
		while(matcher.find())
		{
			String data = matcher.group();
			data = data.substring(1,data.length()-1);
			System.out.println(data);
			// put something to strip the @
			paramHash.put(data, Constants.EMPTY);
		}
		return paramHash;
	}
	
	public static String fillParam(String query, Hashtable paramHash)
	{
		// Hashtable is of pattern <String to be replaced> <replacement>
		// key will be surrounded with @ just to be in sync
		Enumeration keys = paramHash.keys();
		while(keys.hasMoreElements())
		{
			String key = (String)keys.nextElement();
			String value = (String)paramHash.get(key);
			if(!value.equalsIgnoreCase(Constants.EMPTY))
				query = query.replace("@" + key + "@", value);
		}
		//System.out.println("Query is " + query);
		return query;
	}
	
	public static String getInstanceName(String uri)
	{
		StringTokenizer tokens = new StringTokenizer(uri + "", "/");
		int totalTok = tokens.countTokens();
		String className = null;
		String instanceName = null;

		for (int tokIndex = 0; tokIndex <= totalTok && tokens.hasMoreElements(); tokIndex++) {
			if (tokIndex + 2 == totalTok)
				className = tokens.nextToken();
			else if (tokIndex + 1 == totalTok)
				instanceName = tokens.nextToken();
			else
				tokens.nextToken();

		}
		return instanceName;
	}

	public static String getClassName(String uri)
	{
		// there are three patterns
		// one is the /
		// the other is the #
		// need to have a check upfront to see 
		
		StringTokenizer tokens = new StringTokenizer(uri + "", "/");
		int totalTok = tokens.countTokens();
		String className = null;
		String instanceName = null;

		for (int tokIndex = 0; tokIndex <= totalTok && tokens.hasMoreElements(); tokIndex++) {
			if (tokIndex + 2 == totalTok)
				className = tokens.nextToken();
			else if (tokIndex + 1 == totalTok)
				instanceName = tokens.nextToken();
			else
				tokens.nextToken();

		}
		return className;

	}
	
	public static String getNextID()
	{
		id++;
		return Constants.BLANK_URL + "/" + id;
	}

	public static String getQualifiedClassName(String uri)
	{
		// there are three patterns
		// one is the /
		// the other is the #
		// need to have a check upfront to see 
		
		String instanceName = getInstanceName(uri);
		
		String className = getClassName(uri);
		String qualUri ="";
		if(uri.indexOf("/") >= 0)
			instanceName = "/" + instanceName;
		// remove this in the end
		if(className==null)
		{
			qualUri = uri.replace(instanceName, "");
		}
		else
		{
			qualUri = uri.replace(className+instanceName, className);
		}
		
		return qualUri;

	}

	
	public static boolean checkPatternInString(String pattern, String string)
	{
		// ok.. before you think that this is so stupid why wont you use the regular java.lang methods.. consider the fact that this could be a ; delimited pattern
		boolean matched = false;
		StringTokenizer tokens = new StringTokenizer(pattern, ";");
		while(tokens.hasMoreTokens() && !matched)
			matched = string.indexOf(tokens.nextToken()) >= 0;
		
		return matched;	
	}
	
	public static Vector<String> convertEnumToArray(Enumeration enums, int size)
	{
		Vector<String> retString = new Vector<String>();
		for(int count = 0;enums.hasMoreElements();retString.add((String)enums.nextElement()));
		return retString;
	}

	public static boolean runCheck(String query){
		boolean check =true;
		
		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		// get the selected repository
		Object [] repos = (Object [])list.getSelectedValues();
		
		SesameJenaSelectWrapper selectWrapper = null;
		for(int repoIndex = 0;repoIndex < repos.length;repoIndex++)
		{
			IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repos[repoIndex]+"");
			// use the layout to load the sheet later

			selectWrapper = new SesameJenaSelectWrapper();
			selectWrapper.setEngine(engine);
			selectWrapper.setQuery(query);
			selectWrapper.executeQuery();
		}
		//if the wrapper is not empty, calculations have already been performed.
		if(selectWrapper.hasNext()) check = true;
		else check = false;
		return check;
	}
	
	public static void showError(String text){
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		JOptionPane.showMessageDialog(playPane, text, "Error", JOptionPane.ERROR_MESSAGE);
		
	}
	public static void showConfirm(String text){
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		JOptionPane.showConfirmDialog(playPane, text);
		
	}
	public static void showMessage(String text){
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		JOptionPane.showMessageDialog(playPane, text);
		
	}
	public static double round(double valueToRound, int numberOfDecimalPlaces)
	{
	    double multipicationFactor = Math.pow(10, numberOfDecimalPlaces);
	    double interestedInZeroDPs = valueToRound * multipicationFactor;
	    return Math.round(interestedInZeroDPs) / multipicationFactor;
	}
	
	public static String sciToDollar(double valueToRound)
	{
		double roundedValue = Math.round(valueToRound);
		DecimalFormat df = new DecimalFormat("#0");
		NumberFormat formatter = NumberFormat.getCurrencyInstance();
		df.format(roundedValue);
		String retString = formatter.format(roundedValue);
		return retString;
	}
	
	
	public static IEngine loadEngine(String fileName, Properties prop)
	{
		IEngine engine = null;
		
		try {
			String engines = DIHelper.getInstance().getLocalProp(Constants.ENGINES) + "";

			//Properties prop = new Properties();
			//prop.load(new FileInputStream(fileName));
			String engineName = prop.getProperty(Constants.ENGINE);
			String engineClass = prop.getProperty(Constants.ENGINE_TYPE);
			engine = (IEngine)Class.forName(engineClass).newInstance();
			engine.openDB(fileName);
			engine.setEngineName(engineName);
			// set the core prop
			DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.DREAMER, prop.getProperty(Constants.DREAMER));
			DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.ONTOLOGY, prop.getProperty(Constants.ONTOLOGY));
			if(prop.containsKey(Constants.OWL))
				DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.OWL, prop.getProperty(Constants.OWL));
			// set the engine finally
			engines = engines + ";" + engineName;
			DIHelper.getInstance().setLocalProperty(engineName, engine);
			DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engines);
		}  catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return engine;

	}
	
	

}
