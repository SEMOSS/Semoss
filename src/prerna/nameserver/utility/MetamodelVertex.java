package prerna.nameserver.utility;

import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * class to simplify the implementation
 * of combining alias names and properties 
 * on vertices
 * @author mahkhalil
 *
 */
public class MetamodelVertex {
	
	// store the property conceptual names
	private Set<String> propSet = new TreeSet<String>();
	// store the key conceptual names
	private Set<String> keySet = new TreeSet<String>();
	// the conceptual name for the concept
	private String conceptualName;
	
	public MetamodelVertex(String conceptualName) {
		this.conceptualName = conceptualName;
	}
	
	/**
	 * Add to the properties for the vertex
	 * @param propertyConceptual
	 * @param propertyAlias
	 */
	public void addProperty(String propertyConceptual) {
		if (propertyConceptual.equals("noprop")) {
			return;
		}
		propSet.add(propertyConceptual);
	}
	
	/**
	 * Add the keys for the vertex
	 * @param keyConceptual
	 */
	public void addKey(String keyConceptual) {
		if (keyConceptual.equals("nokey")) {
			return;
		}
		keySet.add(keyConceptual);
	}
	
	public Map<String, Object> toMap() {
		Map<String, Object> vertexMap = new Hashtable<String, Object>();
		vertexMap.put("conceptualName", this.conceptualName);
		vertexMap.put("propSet", this.propSet);
		vertexMap.put("keySet", this.keySet);
		return vertexMap;
	}
	
	public String toString() {
		return toMap().toString();
	}
}