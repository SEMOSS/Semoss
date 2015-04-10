package prerna.ds;

import java.util.Collections;
import java.util.Hashtable;
import java.util.Vector;

public class BuilderAPI {
	
	// count of total number of columns in this
	// find the unique values in a column
	// range for a given type
	// total number of records
	// other function that can be performed
	
	// sets the reference to the builder
	SimpleTreeBuilder builder = null;
	
	// gets all the columns
	public Vector <String> getColumns()
	{
		
		if(builder.finalChildType == null)
			return null;
		Vector <String> retVector = new Vector<String>();
		
		TreeNode aNode = builder.nodeIndexHash.get(builder.finalChildType);
		SimpleTreeNode sNode = aNode.instanceNode.elementAt(0);
		while(sNode != null)
		{
			retVector.add(((ISEMOSSNode)sNode.leaf).getType());
			sNode = sNode.parent;
		}
		Collections.reverse(retVector);
		return retVector;
	}
	
	// get the number of nodes / keys for a given column
	public Vector <ISEMOSSNode> getColumn(String column)
	{
		// gets all the instance nodes
		return builder.getSInstanceNodes(column);
	}
	
	// get the number of unique keys with count
	public Hashtable <String, Integer> getUniqueItems4Column(String column)
	{
		Vector <ISEMOSSNode> instances =  builder.getSInstanceNodes(column);
		Hashtable <String,Integer> retHash = new Hashtable <String, Integer>();
		for(int instanceIndex = 0;instanceIndex < instances.size();instanceIndex++)
		{
			ISEMOSSNode aNode = instances.get(instanceIndex);
			String key = aNode.getKey();
			Integer num = 0;
			if(retHash.containsKey(key))
				num = retHash.get(key);
			num++;
			retHash.put(key, num);
		}
		return retHash;		
	}
	
	// find the range
	// returns a string of keys
	// if something else is desired, we can pass it in
	public Vector <String> getItemsInRange(String column, String propName, double topRange, double bottomRange)
	{
		Vector <ISEMOSSNode> instances =  builder.getSInstanceNodes(column);
		Vector <String> output = new Vector<String>();
		for(int instanceIndex = 0;instanceIndex < instances.size();instanceIndex++)
		{
			ISEMOSSNode aNode = instances.get(instanceIndex);
			// I need to get this from the redis
			// get the property from the node
			// check if the range is there or not
			// if so add it to the 
			// output
		}
		return output;
	}
	
	// set up the builder
	public void setBuilder(SimpleTreeBuilder builder)
	{
		this.builder = builder;
	}

	
	
	
}
