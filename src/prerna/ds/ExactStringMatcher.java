package prerna.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IMatcher;
import prerna.algorithm.api.ITableDataFrame;
import prerna.om.SEMOSSParam;

public class ExactStringMatcher implements IMatcher {

	private static final Logger LOGGER = LogManager.getLogger(ExactStringMatcher.class.getName());
	
	public final String COLUMN_ONE_KEY = "table1Col";
	public final String COLUMN_TWO_KEY = "table2Col";
	
	protected List<SEMOSSParam> options;
	protected Map<String, Object> resultMetadata = new HashMap<String, Object>();
	
	public ExactStringMatcher(){
		this.options = new ArrayList<SEMOSSParam>();
		
		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(this.COLUMN_ONE_KEY);
		options.add(0, p1);

		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(this.COLUMN_TWO_KEY);
		options.add(1, p2);
	}
	
	@Override
	public void setSelectedOptions(Map<String, Object> selected) {
		Set<String> keySet = selected.keySet();
		for(String key : keySet)
		{
			for(SEMOSSParam param : options)
			{
				if(param.getName().equals(key)){
					param.setSelected(selected.get(key));
					break;
				}
			}
		}
	}

	@Override
	public List<SEMOSSParam> getOptions() {
		return this.options;
	}

	@Override
	public List<TreeNode[]> runAlgorithm(ITableDataFrame... data) {
		if(data.length != 2) {
			throw new IllegalArgumentException("Input data does not contain exactly 2 ITableDataFrames");
		}
		
		String table1Header = (String) options.get(0).getSelected();
		String table2Header = (String) options.get(1).getSelected();
		if(table1Header == null) {
			throw new IllegalArgumentException("Table 1 Column Header is not specified under " + COLUMN_ONE_KEY + " in options");
		} 
		if(table2Header == null) {
			throw new IllegalArgumentException("Table 2 Column Header is not specified under " + COLUMN_TWO_KEY + " in options");
		}
		
		ITableDataFrame table1 = data[0];
		ITableDataFrame table2 = data[1];
		
		LOGGER.info("Getting from first table column " + table1Header);
//		Object[] table1Col = table1.getColumn(table1Header);
//		Object[] table1Col = table1.getUniqueValues(table1Header);
		TreeNode root = ((BTreeDataFrame) table1).getBuilder().nodeIndexHash.get(table1Header);
		IndexTreeIterator treeLevelIterator1 = new IndexTreeIterator(root);

		LOGGER.info("Getting from second table column " + table2Header);
//		Object[] table2Col = table2.getColumn(table2Header);
//		Object[] table2Col = table2.getUniqueValues(table2Header);
		TreeNode root2 = ((BTreeDataFrame) table2).getBuilder().nodeIndexHash.get(table2Header);
		IndexTreeIterator treeLevelIterator2 = new IndexTreeIterator(root2);

		List<TreeNode[]> results = performMatch(treeLevelIterator1, treeLevelIterator2);
		
		return results;
	}

	protected List<TreeNode[]> performMatch(IndexTreeIterator it1, IndexTreeIterator it2) {
		
		List<TreeNode[]> results = new ArrayList<TreeNode[]>();
		
		boolean thereAreNext = it1.hasNext() && it2.hasNext();
		//get initial value 1
		TreeNode nextNode1 = it1.next();
//		String key1 = nextNode1.leaf.getValue().toString();
		//get initial value 2
		TreeNode nextNode2 = it2.next();
//		String key2 = nextNode2.leaf.getValue().toString();
		
		while(thereAreNext){
			
			
			int comp = compare(nextNode1, nextNode2);
			// -1 means key1 is ahead
			// 0 means they match
			// 1 means key 2 is ahead
			
			// if key1 is ahead, get next for key2
			// if no next for key2, we are done
			if(comp == -1){
				if(it2.hasNext()){
					nextNode2 = it2.next();
//					key2 = nextNode2.leaf.getValue().toString();
					continue;
				}
				else {
					thereAreNext = false;
				}
			}
			// same logic for key2 ahead
			else if(comp == 1){
				if(it1.hasNext()){
					nextNode1 = it1.next();
//					key1 = nextNode1.leaf.getValue().toString();
					continue;
				}
				else {
					thereAreNext = false;
				}
			}
			// if we have a match, add row to the return
			else if (comp == 0){
//				addMatches(results, nextNode1, nextNode2);

				TreeNode[] row = new TreeNode[2];
				row[0] = nextNode1;
				row[1] = nextNode2;
				results.add(row);
				
				if(it1.hasNext() && it2.hasNext()){
					nextNode1 = it1.next();
					nextNode2 = it2.next();
					continue;
				}
				else {
					thereAreNext = false;
				}
			}
		}
		
		return results;
	}
	
//	private void addMatches(List<SimpleTreeNode[]> results, TreeNode n1, TreeNode n2){
//		List<SimpleTreeNode> instances1 = n1.getInstances();
//		List<SimpleTreeNode> instances2 = n2.getInstances();
//		for(SimpleTreeNode inst1 : instances1){
//			for(SimpleTreeNode inst2 : instances2){
//				SimpleTreeNode[] row = new SimpleTreeNode[2];
//				row[0] = inst1;
//				row[1] = inst2;
//				results.add(row);
//			}
//		}
//	}
	
	protected int compare(TreeNode obj1, TreeNode obj2){
		if(obj1.leaf.isEqual(obj2.leaf)){
			return 0;
		}
		else if(obj1.leaf.isLeft(obj2.leaf)){ // this means node 2 is ahead
			return 1;
		}
		return -1; // else node 1 is ahead
	}

	@Override
	public String getName() {
		return "Exact String Matcher";
	}

	@Override
	public String getDefaultViz() {
		return null;
	}

	@Override
	public List<String> getChangedColumns() {
		return null;
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		return this.resultMetadata;
	}

	@Override
	public String getResultDescription() {
		return "This routine matches matches objects by calling .toString and then comparing using .equals method";
	}
}
