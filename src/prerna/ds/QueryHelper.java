package prerna.ds;

public class QueryHelper {

	/*
	 * This is how the operation proceeds
	 * the query helper is either given one node at a time which is great
	 * or just given a set of columns
	 * it uses these columns to find out how to get the specific data
	 * given that we are constructing a tree, we need to ensure that it is thread safe
	 * there are 2 subroutines in the construction
	 * Routine 1 - Building the tree
	 * each node build can be parallelized
	 * ideally, the way we would do this is split the key value pairs
	 * then run those many threads will be created
	 * at every level, I will do the same thing
	 * Take the parent set of nodes as keys, and then ask it to construct a full fledged tree
	 * 
	 */
	
}
