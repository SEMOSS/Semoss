package prerna.masterdatabase.utility;

import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * class to simplify the implementation
 * of combining alias names and properties 
 * on vertices
 *
 */
public class MetamodelVertex {
	
	// store the property conceptual names
	private Set<String> propSet = new TreeSet<String>();
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
	
	public Map<String, Object> toMap() {
		Map<String, Object> vertexMap = new Hashtable<String, Object>();
		vertexMap.put("conceptualName", this.conceptualName);
		vertexMap.put("propSet", this.propSet);
		return vertexMap;
	}
	
	public String toString() {
		return toMap().toString();
	}
	
	public String getConceptualName() {
		return this.conceptualName;
	}
	
	public Set<String> getPropSet() {
		return this.propSet;
	}
}