package prerna.om;

import java.util.Hashtable;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;

import prerna.util.Constants;
import prerna.util.Utility;


public class DBCMVertex{
	
	
	String uri = null;
	Hashtable <String, Object> propHash = new Hashtable<String,Object>();
	transient Hashtable <String, String>uriHash = new Hashtable<String,String>();
	Vector <DBCMEdge> inEdge = new Vector<DBCMEdge>();
	Vector <DBCMEdge> outEdge = new Vector<DBCMEdge>();
	
	transient Logger logger = Logger.getLogger(getClass());
	
	// TODO need to find a way to identify the source i.e. put that as a property
	
	public DBCMVertex(String uri)
	{
		this.uri = uri;
		putProperty(Constants.URI, uri);
		
		// parse out all the oth er properties
		logger.debug("URI " + uri);
		StringTokenizer tokens = new StringTokenizer(uri + "", "/");
		int totalTok = tokens.countTokens();
		String className = Utility.getClassName(uri);
		String instanceName = Utility.getInstanceName(uri);
		
		logger.debug("Class Name " + className + " Instance Name " + instanceName);

		if(instanceName == null)
			instanceName = uri;
		if(className == null)
			className = instanceName;
		
		putProperty(Constants.VERTEX_TYPE, className);
		logger.debug("Type is " + className);
		
		putProperty(Constants.VERTEX_NAME, instanceName);
		logger.debug("Name is " + instanceName);

	}
	
	public Hashtable getProperty()
	{
		return this.propHash;
	}
	
	public void addInEdge(DBCMEdge edge)
	{
		inEdge.add(edge);
		Integer edgeCount = new Integer(0);
		if(propHash.containsKey(Constants.INEDGE_COUNT))
			edgeCount = (Integer)propHash.get(Constants.INEDGE_COUNT);
		edgeCount++;
		propHash.put(Constants.INEDGE_COUNT, edgeCount);
		
		addVertexCounter(edge.inVertex);
	}
	
	public void addVertexCounter(DBCMVertex outVert)
	{
		// also create specific 
		// find the type
		// get the node on other side
		String vertType = (String)outVert.getProperty(Constants.VERTEX_TYPE);
		//System.out.println("Vertex Type is >>>>>>>>>>>>>>>>>" + vertType);
		Integer vertTypeCount = new Integer(0);
		if(propHash.containsKey(vertType))
			vertTypeCount = (Integer)propHash.get(vertType);	
		vertTypeCount++;
		propHash.put(vertType, vertTypeCount);
	}
	
	
	public void addOutEdge(DBCMEdge edge)
	{
		outEdge.add(edge);
		Integer edgeCount = new Integer(0);
		if(propHash.containsKey(Constants.OUTEDGE_COUNT))
			edgeCount = (Integer)propHash.get(Constants.OUTEDGE_COUNT);
		edgeCount++;
		propHash.put(Constants.OUTEDGE_COUNT, edgeCount);

		addVertexCounter(edge.outVertex);
	}
	
	public Vector<DBCMEdge> getInEdges()
	{
		return this.inEdge;
	}

	public Vector<DBCMEdge> getOutEdges()
	{
		return this.outEdge;
	}

	public String getURI()
	{
		return uri;
	}

	public Object getProperty(String arg0) {
		// TODO Auto-generated method stub
		return propHash.get(arg0);
	}

	public Set<String> getPropertyKeys() {
		// TODO Auto-generated method stub
		return null;
	}

	public Object removeProperty(String arg0) {
		// TODO Auto-generated method stub
		return propHash.remove(arg0);
	}

	public void putProperty(String propName, String propValue)
	{
		propHash.put(propName, propValue);
	}
	
	public void setProperty(String propNameURI, Object propValue) {
		// TODO Auto-generated method stub
		// one is a p
		StringTokenizer tokens = new StringTokenizer(propNameURI + "", "/");
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
		uriHash.put(instanceName, propNameURI);
		// I need to convert these decimals and other BS into a proper value
		// awesome !!
		// will come to this in a bit
		logger.debug(instanceName + "<>" + propValue);

		// need to write the routine for conversion here
		
		boolean converted = false;
		try
		{
			if(propValue instanceof Literal)
			{
				//logger.info("This is a literal impl >>>>>> "  + ((Literal)propValue).doubleValue());
				propHash.put(instanceName, ((Literal)propValue).doubleValue());
				converted = true;
			}
		}catch(Exception ex)
		{
			logger.debug(ex);
		}
		try
		{
			if(propValue instanceof com.hp.hpl.jena.rdf.model.Literal)
			{
				logger.info("Class is " + propValue.getClass());
				// try double
				try
				{
					Double value = ((com.hp.hpl.jena.rdf.model.Literal)propValue).getDouble();
					converted = true;
					propHash.put(instanceName, value);
				}catch (Exception ignored){	converted = false;	}
				
				// try integer
				if(!converted)
				{
					try
					{
						Integer value = ((com.hp.hpl.jena.rdf.model.Literal)propValue).getInt();
						converted = true;
						propHash.put(instanceName, value);
					}catch (Exception ignored) {converted = false;}
				}
				
				// try boolean
				if(!converted)
				{
					try
					{
						Boolean value = ((com.hp.hpl.jena.rdf.model.Literal)propValue).getBoolean();
						converted = true;
						propHash.put(instanceName, value);
					}catch (Exception ignored) {}
				}
				// try string
				if(!converted)
				{
					try
					{
						String value = ((com.hp.hpl.jena.rdf.model.Literal)propValue).getString();
						converted = true;
						propHash.put(instanceName, value);

					}catch (Exception ignored) {}
				}
				
				//propHash.put(instanceName, ((com.hp.hpl.jena.rdf.model.Literal)propValue).getDouble());
				//converted = true;
			}
		}catch(Exception ex)
		{
			logger.debug(ex);
		}
		if(!converted)
		{
			propHash.put(instanceName, propValue);
		}
		logger.debug(uri + "<>" + instanceName + "<>" + propValue);
	}
}
