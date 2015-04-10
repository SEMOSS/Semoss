package prerna.ds;

public class SampleHashEngine implements ILinkeableEngine {
	
	// takes a parent node
	// and generates a series of child nodes for it
	// for the parent node it takes
	// I just need the level
	// for the child nodes, I just need the from and to number
	// node 1, node 2, node 3
	// childs
	// cnode1.. cnode 20
	
	
	int childLevelStart;
	int childLevelEnd;
	int parLevel = 0;
	int childLevel = 1;
	public static final String TYPE = "TYPE";
	public static final String NODE = "NODE";
	
	
	public SampleHashEngine(int childLevelStart, int childLevelEnd, int parLevel, int childLevel)
	{
		this.childLevelEnd = childLevelEnd;
		this.childLevelStart = childLevelStart;
		this.childLevel = childLevel;
		this.parLevel = parLevel;
	}
	
	public void setStartEnd(int childLevelStart, int childLevelEnd)
	{
		this.childLevelEnd = childLevelEnd;
		this.childLevelStart = childLevelStart;
	}
	
	public void setParLevel(int parLevel)
	{
		this.parLevel = parLevel;
	}
	
	public void setChildLevel(int childLevel)
	{
		this.childLevel = childLevel;
	}
	
	public void setLevel(int parLevel)
	{
		this.parLevel = parLevel;
	}

	@Override
	public String[] getRelatedTypes(String nodeType) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ISEMOSSNode[][] getRelatedNodes(ISEMOSSNode[] fromNodes, String toNodeType) {
		
		// create one child sequence and then set it up for everything
		ISEMOSSNode [] childs = new ISEMOSSNode[childLevelEnd - childLevelStart];
		for(int childIndex = childLevelStart;childIndex < childLevelEnd;childIndex++)
		{
			childs[childIndex] = new StringClass(NODE+childIndex,TYPE+childLevel);
		}
		
		// TODO Auto-generated method stub
		ISEMOSSNode [][] retString = new ISEMOSSNode[fromNodes.length][childLevelEnd - childLevelStart];
		for(int parentIndex = 0;parentIndex < fromNodes.length;parentIndex++)
		{
			// do the child now
				retString[parentIndex] = childs;
		}
		return retString;
	}
}
