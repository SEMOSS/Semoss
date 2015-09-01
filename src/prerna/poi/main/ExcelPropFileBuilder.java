package prerna.poi.main;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class ExcelPropFileBuilder {
	
	private Map<String, String> sheetStart;
	private Map<String, String> sheetEnd;
	private Map<String, StringBuilder> sheetRelMap;
	private Map<String, StringBuilder> sheetNodePropMap;

	private Hashtable<String, String> propHash = new Hashtable<String, String>();
		
	private StringBuilder propFile = new StringBuilder();

	
	public void addStartRow(String sheetName, String value){
		sheetStart.put(sheetName, value);
	}

	public void addEndRow(String sheetName, String value){
		sheetEnd.put(sheetName, value);
	}
	
	public void addProperty(String sheetName, ArrayList<String> sub, ArrayList<String> obj, String dataType) {
		Iterator<String> subIt = sub.iterator();
		String subject = subIt.next();
		while (subIt.hasNext()) {
			subject = subject + "+" + subIt.next();
		}

		Iterator<String> objIt = obj.iterator();
		String object = objIt.next();
		while (objIt.hasNext()) {
			object = object + "+" + objIt.next();
		}

		sheetNodePropMap.get(sheetName).append(subject + "%" + object + ";");
	}

	public void addRelationship(String sheetName, ArrayList<String> sub, String pred, ArrayList<String> obj) {
		Iterator<String> subIt = sub.iterator();
		String subject = subIt.next();
		while (subIt.hasNext()) {
			subject = subject + "+" + subIt.next();
		}

		Iterator<String> objIt = obj.iterator();
		String object = objIt.next();
		while (objIt.hasNext()) {
			object = object + "+" + objIt.next();
		}
		
		sheetRelMap.get(sheetName).append(subject + "@" + pred + "@" + object + ";");
	}

	public void constructPropHash() {
		Set<String> sheetNames = new HashSet<String>();
		sheetNames.addAll(sheetStart.keySet());
		sheetNames.addAll(sheetEnd.keySet());
		sheetNames.addAll(sheetRelMap.keySet());
		sheetNames.addAll(sheetNodePropMap.keySet());
		for(String sheet : sheetNames) {
			String cleanSheet = sheet.trim().replaceAll(" ", "_");
			propHash.put(cleanSheet + "_START_ROW", sheetStart.get(sheet));
			propHash.put(cleanSheet + "_END_ROW", sheetEnd.get(sheet));
			propHash.put(cleanSheet + "_RELATION", sheetRelMap.get(sheet).toString());
			propHash.put(cleanSheet + "_NODE_PROP", sheetNodePropMap.get(sheet).toString());
			propHash.put(cleanSheet + "_RELATION_PROP", "");
	
			propFile.append(cleanSheet + "_START_ROW\t" + sheetStart.get(sheet) + "\n");
			propFile.append(cleanSheet + "_END_ROW\t" + sheetEnd.get(sheet) + "\n");
			propFile.append(cleanSheet + "_RELATION\t" + sheetRelMap.get(sheet).toString() + "\n");
			propFile.append(cleanSheet + "_NODE_PROP\t" + sheetNodePropMap.get(sheet).toString() + "\n");
			propFile.append(cleanSheet + "_RELATION_PROP\t" + "\n");
		}
	}

	public Hashtable<String, String> getPropHash() {
		constructPropHash();
		return propHash;
	}

	public String getPropFile() {
		return propFile.toString();
	}
}
