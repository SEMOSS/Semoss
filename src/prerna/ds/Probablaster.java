package prerna.ds;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.openrdf.model.vocabulary.RDF;

import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdf.InMemorySesameEngine;
import prerna.rdf.engine.wrappers.WrapperManager;

public class Probablaster {
	
	// core method to test the idea around construction of a BN
	// TODOS
	// I need to resolve how to do stuff, when I travel upwards
	// and when the system is out of order i..e I go from X1-X3 and want to find child in X2. I have no idea how to do this right now. 
	
	String [] columns = {"X1", "X2", "X3"}; //, "X4", "X5", "X6", "X7", "X8"};
	BTreeDataFrame df = null;
	SimpleTreeBuilder comboBuilder = null;
	Hashtable <String, Double> BICIHash = new Hashtable<String, Double>();
	Hashtable <String, Integer> occurenceHash = new Hashtable <String, Integer>();
	Hashtable <String, Double> penaltyHash = new Hashtable <String, Double>();
	Random rand = new Random();
	
	InMemorySesameEngine engine = new InMemorySesameEngine();
	
	
	Hashtable <String, ISEMOSSNode> keyTreeHash = new Hashtable <String, ISEMOSSNode>();
	
	double logN = 0.0; // I will need to set this up when I do calculate it
	String combos = "";
	boolean firstTime = true;
	double baseBIC = -9999.9999;
	int totalRows = 0;
	
	SimpleTreeNode rootNode = null;
	SimpleTreeNode lasNodeAdded = null;
	List <String> fallBack = new ArrayList<String>();
	
	
	double baseRi = 1.0;
	
	Hashtable <String, String> takenHash2 = new Hashtable<String, String>();

	Hashtable <String, String> triedCombos = new Hashtable<String, String>();
	
	SimpleTreeNode selectedNode = null;
	
	
	public void testURL()
	{
		String key = "1.%201d83819b-10b0-406a-81d0-51ed4cbd56d2";

		System.out.println(key);
		key = URLEncoder.encode(key);
		System.out.println(key);
		key = URLDecoder.decode(key);
		System.out.println(key);
	}
	
	public void setDataFrame(BTreeDataFrame df)
	{
		this.df = df;
		// bin all the numeric columns
		df.binAllNumericColumns();
		// get the column names based on the binning
		columns = df.levelNames;
		composeColumns();
	}
	
	public void composeColumns()
	{
		// the main objective of this method is to find
		// which one of the columns are numeric and remove them from the list
		Vector <String> nonNumericColumns = new Vector<String>();
		boolean [] numColumns = df.isNumeric();
		for(int colIndex = 0;colIndex < numColumns.length;colIndex++)
		{
			if(!numColumns[colIndex])
				nonNumericColumns.add(columns[colIndex]);			
		}
		
		// convert the columns back
		columns = new String[nonNumericColumns.size()];
		nonNumericColumns.copyInto(columns);
	}
	
	
	public void makeData()
	{
		if(df == null)
		{
			df = new BTreeDataFrame(columns);
			String [][] output = {
			{"a", "1", "m"},
			{"b", "1", "n"},
			{"a", "2", "n"},
			{"c", "3", "n"},
			{"b", "1", "m"},
			{"c", "1", "m"},
			{"a", "2", "n"},
			{"d", "3", "n"},
			{"d", "3", "n"},
			{"d", "3", "m"},
			{"a", "2", "m"},
			{"b", "1", "m"}
			};
						
			// add my rows
			for(int dIndex = 0;dIndex < output.length;dIndex++)
			{
				String [] rec = output[dIndex];
				Map <String, Object> recMap = new Hashtable<String, Object>();

				for(int colIndex = 0;colIndex < columns.length;colIndex++)
				{
					recMap.put(columns[colIndex], rec[colIndex]);
				}
				df.addRow(recMap);
			}
		}	
	}
	
	public static void patternTest()
	{
		Pattern pattern = Pattern.compile("\\d*-\\d*");
		String tester = "0:0-1:2:2-3:4:44:44-55";
		Matcher matcher = pattern.matcher(tester);
		System.out.println("Number of Groups.. " + matcher.groupCount());
		int count = 0;
		while(matcher.find())
		{
			count++;
			System.out.println(matcher.start() + " :  " + matcher.end());
			System.out.println(matcher.group());
		}
		
		System.out.println("Count..  " + count);
	}
	
	public void tryPattern()
	{
		String pattern = "";
		System.out.println("Levels are...  " + df.simpleTree.findLevels());
		try {
			do
			{
				System.err.println("Enter a new pattern : ");
				BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
				pattern = reader.readLine();

				int level = 0;
				comboBuilder = new SimpleTreeBuilder("L" + level);
				new StringClass(pattern, "L0");
				System.out.println("Level " + level + " >>> " + level);

				level++;
				comboBuilder.addNode(new StringClass(pattern, "L0"), new StringClass(pattern, "L1"));

				selectedNode = comboBuilder.getRoot();

				play();
				
			} while(!pattern.equalsIgnoreCase("done"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public String runBIC()
	{
		//oscillate();
		
		//tryPattern();
		//String retString = null;
		String pattern = createNewPattern();
		//bs.genAllCombos(pattern);
		
		//makeData();
		//df.simpleTree.getPath("X3", "X1", ";X3;X2;");
		//pattern = "0-1:1-2";
		genAllCombos(pattern);
		SimpleTreeNode finalNode = null;
		
		double finalBIC = -9999.9999;
		
		selectedNode = comboBuilder.getRoot();
		boolean run = true;
		int totalRun = 0;
		while(run)
		{
			run = play();
			if(!run && baseBIC == -9999.9999)
			{
				// shit failed
				// need to create a new pattern
				pattern = createNewPattern();
			}
			else if(run)
				pattern = selectedNode.leaf.getKey();

			System.out.println(" Trying pattern...  " + pattern);
				
			if(run)
			{
				genAllCombos(pattern);
				//firstTime = true;
			}
			
			if(!run && totalRun < 6)
			{
				run = true;
				if(finalNode == null || finalBIC < baseBIC)
				{
					finalNode = selectedNode;
					finalBIC = baseBIC;
				}
				pattern = createNewPattern();
				genAllCombos(pattern);				
				selectedNode = comboBuilder.getRoot();
				totalRun++;
			}
		}
		System.out.println("And.. the winner is......  " + finalNode.leaf.getKey() + "    with a BIC of.. " + baseBIC);

		String retString = finalNode.leaf.getKey().replace("-", "-->") + " <<>> " + baseBIC;
		retString = retString + "\n" + translateToCol(finalNode.leaf.getKey());
		
		return retString;
	}
	
	public void oscillate()
	{
		// first generate the pattern for everything disconnected
		int numEdges = 0;
		
		int n = (columns.length * (columns.length-1))/2;

		double newBic = -9999.9999;
		String parentPattern = "";
		String newPattern = "";
		
		System.out.println("N is .. " + n);
		//numEdges = 6;
		
		do
		{
			String pattern = createNewPattern(numEdges);
			System.out.println("Trying pattern " + pattern);
			makeNewTree(pattern);
			play();
			numEdges++;
			if(baseBIC > newBic || newBic == -9999.9999)
			{
				parentPattern = newPattern;
				newPattern = pattern;
				newBic = baseBIC;
			}
			//else if(newBic != -9999.9999)
			//{
				//parentPattern = newPattern;
			//	break;
			//}	
		}while(numEdges < n);
		
		System.out.println("Seems to stabilize at " + newPattern);
		
		genAllCombos(parentPattern);
		
		selectedNode = comboBuilder.getRoot();

		System.out.println("Travelling side ways.... ");
		
		boolean run = true;
		while(run)
		{
			run = play();
			if(!run && baseBIC == -9999.9999)
			{
				// shit failed
				// need to create a new pattern
				newPattern = createNewPattern();
			}
			else if(run)
				newPattern = selectedNode.leaf.getKey();

			System.out.println(" Trying pattern...  " + newPattern);
				
			if(run)
			{
				genAllCombos(newPattern);
				//firstTime = true;
			}
			
		}
		
		System.out.println("winner is...  " + selectedNode.leaf.getKey());
	}
	
	public void makeNewTree(String pattern)
	{
		int level = 0;
		comboBuilder = new SimpleTreeBuilder("L" + level);
		new StringClass(pattern, "L0");
		System.out.println("Level " + level + " >>> " + level);

		level++;
		comboBuilder.addNode(new StringClass(pattern, "L0"), new StringClass(pattern, "L1"));

		selectedNode = comboBuilder.getRoot();

	}
	
	
	
	public static void main(String [] args)
	{
		//patternTest();
		Probablaster bs = new Probablaster();
		bs.makeData();
		bs.oscillate();
		/*
		//bs.testURL();
		
		//System.out.println(bs.translateToCol("0-1:0-2"));
		
		String pattern = bs.createNewPattern();
		pattern = "0:1:2";
		bs.genAllCombos(pattern);
		
		bs.makeData();
		bs.composeColumns();
		//bs.df.simpleTree.getPath("X3", "X1", ";X3;X2;");
		//pattern = "0-1:1-2";
		//bs.genAllCombos(pattern);
		
		bs.selectedNode = bs.comboBuilder.getRoot();
		boolean continueToNext = true;
		while(continueToNext)
		{
			continueToNext = bs.play();
			if(continueToNext)
			{
				pattern = bs.selectedNode.leaf.getKey();
				bs.genAllCombos(pattern);
			}
		}
		System.out.println("And.. the winner is......  " + bs.selectedNode.leaf.getKey() + "    with a BIC of.. " + bs.baseBIC);
		
		//bs.createNewPattern();
		// make the data first
		/*
		
		bs.genAllCombos();
		
		
		
		System.out.println(bs.comboBuilder.nodeIndexHash);
		Random rand = new Random(System.currentTimeMillis());

		int height= 0;
		int breadth = 0;
		
		//int height = rand.nextInt(bs.columns.length) - 1;
		
		//int totalCount = bs.comboBuilder.getNodeTypeCount("L" + height);
		//int breadth = rand.nextInt(totalCount) - 1;
		
		System.out.println("Selecting Random at " + height + " count.  " + breadth);
		
		SimpleTreeNode daNode = bs.comboBuilder.getRoot();
		int count = 0;
		while(count < height)
		{
			daNode = daNode.leftChild;
			count++;
		}
		
		
		// now the breadth
		count = 0;
		while(count < breadth)
		{
			daNode = daNode.rightSibling;
			count++;
		}
		bs.selectedNode = daNode;
		System.out.println("Value to start is " + daNode.leaf.getKey());
		bs.translateToCol(daNode.leaf.getKey());
		boolean stop = false;
		while(!stop)
			stop = bs.play();
		
		System.out.println("And.. the winner is......  " + bs.selectedNode.leaf.getKey() + "    with a BIC of.. " + bs.baseBIC);
		
		//Hashtable <String, Hashtable<String, Integer>> data =  bs.df.simpleTree.getPath("X2", "X1");
		//Hashtable <String, Hashtable<String, Integer>> data =  bs.df.simpleTree.getPath("X1", "X3", ";X1;X2;");
		//Hashtable <String, Hashtable<String, Integer>> flippedTable = bs.df.simpleTree.flipPath(data);
		
		//bs.play();
		
		//bs.findNet();
		
		//bs.genCombos("0");
		//bs.findBIC("X1", "X2");
		
		/*
		// printing some base data
		System.out.println("Unique columns .. i.e. Ri");
		for(int colIndex = 0;colIndex < bs.columns.length;colIndex++)
			System.out.println(bs.df.getUniqueInstanceCount(bs.columns[colIndex]));
		
		// weights test
		// get the matrix
		Hashtable <String, Hashtable<String,Integer>> childs = bs.df.simpleTree.getPath("X1", "X2");
		// get the unique instances
		
		Hashtable <String, Integer> nodeOccurences = bs.df.simpleTree.getNodeOccurence("X1");
		
		Object [] x3Instances = bs.df.getUniqueValues("X2");
		
		// get the number of repeats
		double crossTab = bs.getCrossTab(childs, x3Instances, "d");
*/		
		/*
		 * A - A-B, A-C
		 * B - B-A, B-C
		 * C- C-B, C-A
		 * 
		 * A-B - A- B- C, A-B A-C, C-A-B
		 * A-C - A-C-B, A-C A-B, B-A-C
		 * B-A = B-A B-C, B-A-C, C-B-A
		 * B-C - B-C-A, B-C B-A, A-B-C
		 * C-B - C-B-A, A-C-B, C-B C-A
		 * 
		 */
		System.out.println("Done");		
	}
	
	public String createNewPattern()
	{
		int n = (columns.length * (columns.length-1))/2;
		int noOfEdges = rand.nextInt(n);
		return createNewPattern(noOfEdges);
	}
	
	public String createNewPattern(int noOfEdges)
	{
		Vector<SimpleTreeNode> retNode = null;
		
		//Random rand = new Random();
		int n = (columns.length * (columns.length-1))/2;
		
		//int noOfEdges = rand.nextInt(n);
		Hashtable <String, String> edgeHash = new Hashtable();
		
		int numberOfSkips = n - noOfEdges;
		
		// I need to find a pattern with n number of edges
		int edgeCount = 0;
		int skipCount = 0;
		String firstPattern = "";
		boolean straight = false;
		int holder = 0;
		Vector <Integer> addedNodes = new Vector<Integer>();
		while(edgeCount + 1 < columns.length && edgeCount < noOfEdges) // first add everything
		{
			if(firstPattern.length() == 0)
				firstPattern = edgeCount + "-" + (edgeCount+1);
			else
			{
				firstPattern = firstPattern + ":" + edgeCount + "-" + (edgeCount+1);
			}
			if(!addedNodes.contains(edgeCount))
				addedNodes.addElement(edgeCount);
			edgeCount = edgeCount+1;
			if(!addedNodes.contains(edgeCount))
				addedNodes.addElement(edgeCount);
		}
		//edgeCount++;
			
		while(edgeCount < noOfEdges)
		{
			// now add the edges to the alternate nodes
			
			for(int colIndex = holder+2;colIndex < columns.length && edgeCount < noOfEdges;colIndex++)
			{
				firstPattern = firstPattern + ":" + holder + "-" + colIndex;
				edgeCount++;
			}
			holder++;
		}
		
		String remaining = getRemainingNew(addedNodes);
		// finally add the remaining pieces
		if(firstPattern.length() > 0 && remaining.length() > 0)
			firstPattern = firstPattern + ":" + remaining;
		else if(remaining.length() > 0)
			firstPattern = remaining;
		
		
		System.out.println("First Pattern is " + firstPattern);
		//genCombos2(firstPattern, 1);
		
		
		return firstPattern;
	}
	
	public String getRemainingNew(Vector <Integer> hasVector)
	{
		String output = "";
		for(int vecIndex = 0;vecIndex < columns.length;vecIndex++)
		{
			if(!hasVector.contains(vecIndex))
			{
				if(output.length() == 0)
					output = vecIndex + "";
				else
					output = output + ":" + vecIndex;
			}
		}
		return output;
	}
	
	
	public String translateToCol(String input)
	{
		
		// I have no idea why I go through such a complicated routine.. anyways.. will come to it shortly
		// ok.. let us end this stupidity
		
		
		String retString = "";
		StringTokenizer tokens = new StringTokenizer(input,":");
		SimpleTreeNode curNode = null;
		
		engine = new InMemorySesameEngine();
		engine.openDB(null);
		
		// I will also construct the tree here
		while(tokens.hasMoreTokens())
		{
			String comElem = tokens.nextToken();
			String elem = "";
			if(comElem.indexOf("-") >= 0)
			{
				// there is also a chance there is a pipe in here
					String parent = comElem.substring(0,comElem.indexOf("-"));
					String child = comElem.substring(comElem.indexOf("-") + 1);
					String parCol = columns[Integer.parseInt(parent)];
					String childCol = columns[Integer.parseInt(child)];
					elem = parCol + "-" + childCol;
			}
			else
			{
				elem = columns[Integer.parseInt(comElem)];
			}
			if(retString.length() == 0)
				retString = elem;
			else
				retString = retString + ":" + elem;
		}
		
		System.out.println("translated column for input  " + input + "  >>  " + retString);
		return retString;
	}
		
	public void putSesame(String node, String child)
	{
		Object [] objects = new Object[4];
		objects[0] = "temp:"+ node;
		if(child == null)
		{
			objects[1] = RDF.TYPE + "";
			objects[2] = "temp:NODE";
		}
		else
		{
			objects[1] = "rel:child";
			objects[2] = "temp:" + child;
		}
		
		objects[3] = new Boolean(true);
		engine.addStatement(objects);
	}
		
	public void addToRoot(SimpleTreeNode node)
	{
		// see if I need to do this for root
		if(rootNode == null)
			rootNode = node;
		else
			rootNode.addSibling(node);
	}
	
	public void genAllCombos(String pattern)
	{
		int level = 1;
		keyTreeHash.clear();
		String initialCombos =  pattern; //createNewPattern();
		
		/*for(int inIndex = 0;inIndex < columns.length;inIndex++)
		{
			if(initialCombos.length() == 0)
				initialCombos = inIndex + "";
			else
				initialCombos = initialCombos + ":" + inIndex;
		}*/
		Vector <String> nextRound = new Vector<String>();
		nextRound.addElement(initialCombos);
		
		// create simpletree builder
		comboBuilder = new SimpleTreeBuilder("L" + level);
		System.out.println("Level " + level + " >>> " + nextRound);
		
		//while(level <= 1) //columns.length) // only fill one more level
		boolean lastLevel = true;
		{
			level++;
			System.out.println("Creating the childs ");
			Vector <String> nextRound2 = new Vector<String>();
			int size = nextRound.size();
			for(int runner = 0;nextRound.size() > 0 ;runner++)
			{
				//System.out.println(" Runner..  " + runner + "  size..  " + nextRound.size());
				String newData = nextRound.remove(0);				
				nextRound2.addAll(genCombos2(newData, level));
			}
			if(nextRound2.size() > 0)
				lastLevel = false;

			nextRound = nextRound2;
		}
		
		// finally make a parent
		// can only make parent if there is one edge
		if(pattern.indexOf("-") > 0)
		{
			// As an honor to george.. now I select random
			String newPattern = "";
			StringTokenizer patTokens = new StringTokenizer(pattern,":");
			int num = patTokens.countTokens();
			// As an honor to george.. now I select random
			int randomEdgeToRemove = 0 ; //rand.nextInt(num); // get to randomizing this later
			int tracker = 0;
			String remnant = "";
			String remainingPatterns = pattern + ":";
			while(patTokens.hasMoreTokens())
			{
				String token = patTokens.nextToken();
				remainingPatterns = remainingPatterns.replace(token, ""); // take it out of remaining patterns
				remnant = "";
				
				if(token.indexOf("-") > 0)
				{
					// found him
					if(tracker == randomEdgeToRemove)
					{
						String parentToKeep = token.substring(0,token.indexOf("-")); 
						remnant = token.substring(token.indexOf("-") + 1);
						token = parentToKeep;	
					}
					tracker++;
					// do the remaining part
					
					if(checkIfAbsent(token, remainingPatterns))
					{
						if(newPattern.length() > 0)
							newPattern = newPattern + ":" + token;
						else// i need to check if absent piece again here
							newPattern = token;
					}
				}
				else
					remnant = token;
				if(remnant.length() > 0 && checkIfAbsent(remnant, remainingPatterns))
				{
					newPattern = newPattern + ":" + remnant;
				}
			}

			// now add this as the parent
			ISEMOSSNode parNode = new StringClass(newPattern, "L0");
			
			// add this guy as the parent
			// may be keyTreeHash has no parent
			ISEMOSSNode childNode = null;
			if(keyTreeHash.containsKey(pattern))
				childNode = keyTreeHash.get(pattern);
			else
				childNode = new StringClass(pattern, "L1");
			comboBuilder.addNode(parNode, childNode);
			
			System.out.println("Level 0 " + newPattern);
			
			if(!lastLevel)
				selectedNode = comboBuilder.getRoot(); //.leftChild; // get this pattern
			else
			{
				System.out.println("Back into sole searching mode.. but ");
				selectedNode = comboBuilder.getRoot().parent;
				
				// should be more to do with siblings
				genAllCombos(selectedNode.leaf.getKey());
			}
			//ISEMOSSNode curRootNode = (ISEMOSSNode)comboBuilder.getRoot().leaf.getKey();
		}
		
		
		
	}
	
	public boolean checkIfAbsent(String key, String searchString)
	{
		boolean present = searchString.contains(":" + key+ ":");
		present = present || searchString.contains(":" + key + "-");
		present = present || searchString.contains("-" + key + ":");
		return !present;
	}
	
	public Vector<String> genCombos2(String input, int level)
	{
		StringTokenizer elements = new StringTokenizer(input, ":");
		String [] tokens = new String[elements.countTokens()];
		
		// return Vector
		Vector <String> nextRound = new Vector<String>();
		
		// convert it to array
		for(int elemIndex = 0;elemIndex < tokens.length;tokens[elemIndex] = elements.nextToken(),elemIndex++);

		int starter = 0;
		String combos = "";
		System.out.println("Generating Child for " + input);
		while(starter < tokens.length)
		{
			// generate the patterns
			for(int genIndex = 0;genIndex < tokens.length;genIndex++)
			{
				// try to see if this is the same element as before
				if(genIndex != starter)
				{
					// ok.. I might have a new pattern here
					// check to see if this element has an existing 
					if(tokens[starter].indexOf("-") > 0) // generate some of the composite combos i.e. 1-2:0 will get you 1-0:1:2;2-0:1-2
						nextRound = genCompositeCombos(tokens[starter], input, tokens, starter, level, nextRound); 
					else
					{
						// this is easy
						String otherString = tokens[genIndex];
						String newTarget = tokens[genIndex];
						if(otherString.indexOf("-")>0)
						{
							newTarget = otherString.substring(0, otherString.indexOf("-"));
							otherString = ":" + otherString;
						}
						else
							otherString = "";
						
						newTarget = tokens[starter] + "-" + newTarget + otherString;
						String remaining = getRemaining(tokens, starter, genIndex);
						String finalOutput = newTarget + remaining; // + ":" + tokens[starter];
						
						// add it to the combobuilder
						if(!keyTreeHash.containsKey(finalOutput))
						{
							// add this to the combo builder
							ISEMOSSNode parentNode = null;
							if(keyTreeHash.containsKey(input))
								parentNode = keyTreeHash.get(input);
							else
								parentNode = new StringClass(input, "L" + (level -1));
							ISEMOSSNode targetNode = new StringClass(finalOutput, "L" + level);
							keyTreeHash.put(input, parentNode);
							keyTreeHash.put(finalOutput, targetNode);
							nextRound.add(finalOutput);
							
							// add it to the tree
							comboBuilder.addNode(parentNode, targetNode);
						}
					}
				}
			}
			starter++;
		}		
		// try with flips
		if(nextRound.size() == 0)
		{
			// I need to create flips here
			//nextRound = flipAll(input, tokens);
		}

		if(nextRound.size() > 0)
			System.out.println(" Level  "+ level + " >>> " + nextRound);
		return nextRound;
	}
	
	public Vector<String> flipAll(String core, String [] input)
	{
		Vector <String> retVector = new Vector<String>();
		
		int start = 0;
		while(start < input.length)
		{
			String starter = input[start];
			starter = flipIt(starter); // flip the first one first
			
			int numToFlip = 1; // need to find how to flip 2 etc etc.. 
			
			for(int colIndex = 0;colIndex < input.length;colIndex++)
			{
				if(colIndex != start)
				{
					String thisNode = input[colIndex];
					thisNode = flipIt(thisNode);
					String remains = getRemaining(input, start, colIndex);
					String finalStr = starter + ":" + thisNode + ":" + remains;
					retVector.add(finalStr);
					ISEMOSSNode parentNode = null;
					if(keyTreeHash.containsKey(input))
						parentNode = keyTreeHash.get(input);
					else
						parentNode = new StringClass(core, "L" + 0);
					ISEMOSSNode targetNode = new StringClass(finalStr, "L" + 1);
					keyTreeHash.put(core, parentNode);
					keyTreeHash.put(finalStr, targetNode);
					
					
					// add it to the tree
					comboBuilder.addNode(parentNode, targetNode);

				}
			}
			start++;
		}
		return retVector;
	}
	
	public String flipIt(String input)
	{
		String parent = input.substring(0,input.indexOf("-"));
		String child = input.substring(input.indexOf("-")+1);
		return child + "-" + parent;
		
	}
	
	public Vector<String> genCompositeCombos(String start, String fullString, String [] remaining, int startIndex, int level, Vector <String> nextRound)
	{
		StringTokenizer startTokens = new StringTokenizer(start,"-");
		String [] tokens = new String[startTokens.countTokens()]; // created these tokens
		// convert it to array
		String checker = "";
		for(int elemIndex = 0;elemIndex < tokens.length;tokens[elemIndex] = startTokens.nextToken(),checker = checker+"__"+tokens[elemIndex]+"__", elemIndex++);
		int starter = 0;
		while(starter < tokens.length)
		{
			for(int genIndex = 0;genIndex < tokens.length;genIndex++)
			{
				// generate each of these patterns
				if(genIndex != startIndex)
				{
					String newTarget = remaining[genIndex];
					if(!(newTarget.indexOf("-") >= 0)) // this was already taken care before I dont need to do anything here
					{
						String remainingString = getRemaining(remaining, startIndex, genIndex);
						
						String finalOutput = tokens[starter] +"-" + newTarget + remainingString + ":" + start;
						
						//System.out.println("Output is " + finalOutput);
						
						//knownStrings.put(finalOutput, finalOutput);
						
						if(!keyTreeHash.containsKey(finalOutput))
						{
							// add this to the combo builder
							ISEMOSSNode parentNode = null;
							if(keyTreeHash.containsKey(tokens[starter]))
								parentNode = keyTreeHash.get(fullString);
							else
								parentNode = new StringClass(fullString, "L" + (level -1));
							ISEMOSSNode targetNode = new StringClass(finalOutput, "L" + level);
							keyTreeHash.put(finalOutput, targetNode);
							keyTreeHash.put(fullString, parentNode);
							nextRound.add(finalOutput);
							
							// add it to the tree
							comboBuilder.addNode(parentNode, targetNode);
						}
					}
				}
			}
			starter++;
		}
		// return the next round
		return nextRound;
	}	
	
	public String getRemaining(String [] remaining, int startIndex, int genIndex)
	{
		String retString = "";
		for(int remIndex = 0;remIndex < remaining.length;remIndex++)
		{
			if(remIndex != startIndex && remIndex != genIndex)
			{
				if(retString.length() == 0)
					retString = remaining[remIndex];
				else
					retString = retString + ":" + remaining[remIndex];
			}
		}
		if(retString.length() > 0)
			retString = ":" + retString;
		return retString;
	}
	
	/*
	public String genCombos5(String input)
	{
		// input is typically separated by dashes i.e. 1-2
		// when there is more than one then it goes into 1-2:1-3
		// combinations are connected by ; so 1-2;1-3 etc. 
		StringTokenizer components = new StringTokenizer(input,":");
		int [] nums = null;
		Hashtable numHash = new Hashtable();
		String existing = "";
		
		
		if(input.indexOf("-") >= 0)
			existing = ":" + input;

		while(components.hasMoreTokens())
		{
			String component = components.nextToken();
			Vector <Integer> numVec = new Vector<Integer>();
			
			if(component.indexOf("-") >= 0)
			{
				StringTokenizer tokens = new StringTokenizer(component, "-");
				for(int numIndex =0;tokens.hasMoreTokens();numIndex++)
				{
					String token = tokens.nextToken();
					numVec.add(Integer.parseInt(token));
					numHash.put(Integer.parseInt(token),Integer.parseInt(token));
				}
			}			
			// convert the f** vector
			nums = new int[numVec.size()];
			for(int vecIndex = 0;vecIndex < numVec.size();nums[vecIndex]=numVec.elementAt(vecIndex), vecIndex++);

		}
		
		String comboString = "";
		for(int start = 0;start < columns.length;start++)
		{
			if(!numHash.containsKey(start))
			{
				// only do it when this is not in the string already
				if(nums != null && nums.length > 0)
				{
					for(int numIndex = 0;numIndex < nums.length;numIndex++)
					{
						if(comboString.length() == 0)
							comboString = nums[numIndex] + "-" + start;
						else
							comboString = comboString + ";" + nums[numIndex] + "-" + start;
						
						// third for loop.. man I am killing it.. to add the other pieces into it
						for(int otherColIndex = 0;otherColIndex < columns.length;otherColIndex++)
						{
							if(!numHash.containsKey(otherColIndex) && otherColIndex != start)
							{
								comboString = comboString  + ":" + otherColIndex;
							}
						}
						
						// add the existing piece
						comboString = comboString + existing;
					}
				}
				else
				{
					if(comboString.length() == 0)
						comboString = start + "";
					else
						comboString = comboString + ":" + start;
				}
			}
			// else
			// I could really care less about it :)
		}

		System.out.println(comboString);
		
		return comboString;
	}
	
	public void buildCombos3()
	{
		String output = genCombos("");
		int count = 0;
		
		// first time.. dont worry about it
		comboBuilder = new SimpleTreeBuilder("L" + count);
		ISEMOSSNode rootNode = new StringClass(output, "L" + count);
		
		count++;
		// add all the levels
		Vector <ISEMOSSNode> starter = new Vector<ISEMOSSNode>();
		for(int colIndex = 0;colIndex < columns.length;colIndex++)
		{
			ISEMOSSNode col = new StringClass(colIndex+"", "L" + count);
			comboBuilder.addNode(rootNode, col);
			starter.add(col);
		}
		
		// create all the other levels
		count++;
		buildLevel(starter, count);
	}
	
	public void buildLevel(Vector <ISEMOSSNode> parentNodes, int count)
	{
		Vector <ISEMOSSNode> nextLevel = new Vector<ISEMOSSNode>();
		for(int parIndex = 0;parIndex < parentNodes.size();parIndex++)
		{
			String output = genCombos(parentNodes.elementAt(parIndex).getKey());
			StringTokenizer tokens = new StringTokenizer(output, ";");
			while(tokens.hasMoreElements())
			{
				String component = tokens.nextToken();
				ISEMOSSNode childNode = new StringClass(component, "L" + count);
				comboBuilder.addNode(parentNodes.elementAt(parIndex), childNode); // added this combination
				
				if(count-1 != columns.length+1)
				{
					nextLevel.add(childNode);
					System.out.println("Processing level " + count + " columns " + columns.length);
				}
			}
			
		}
		if(nextLevel.size() > 0)
			buildLevel(nextLevel, count++); // recurse the sucker !!

	}
	*/

	// returns true.. if there is no better option has been found
	// returns false.. 
	public boolean play()
	{
		combos = selectedNode.leaf.getKey();
		SimpleTreeNode curSelectedNode = null;
		//combos = translateToCol(combos);
		
		boolean continueToNext = false;
		
		// assimilate the nearest nodes
		// I cannot always put the parent first
		// I need to find which direction it is going and then flip to other direction
		
		Vector <SimpleTreeNode> otherNodes = new Vector<SimpleTreeNode>();
		otherNodes.add(selectedNode);
		if(selectedNode.parent != null)
			otherNodes.add(selectedNode.parent);
		if(selectedNode.rightSibling != null)
			otherNodes.add(selectedNode.rightSibling);
		if(selectedNode.leftSibling != null)
			otherNodes.add(selectedNode.leftSibling);
		if(selectedNode.leftChild != null)
		{
			SimpleTreeNode child = selectedNode.leftChild;
			while(child != null)
			{
				otherNodes.add(child);
				child = child.rightSibling;
			}
		}
		
		for(int nodeIndex = 0;nodeIndex < otherNodes.size();nodeIndex++)
		{
			// for each configuration 
			// find the BIC score for this configuration
			// and then compare and contrast
			String colConfig = otherNodes.get(nodeIndex).leaf.getKey();
			colConfig = translateToCol(colConfig);

			double totalBIC = 0.0;

			if(!BICIHash.containsKey(otherNodes.get(nodeIndex).leaf.getKey()))
			{
				try
				{
					// three possibilities here
					// it has a -
					StringTokenizer config = new StringTokenizer(colConfig, ":");
					// convert to a vector for easier processing
					Vector <String> configVector = new Vector<String>();
					for(;config.hasMoreTokens();configVector.add(config.nextToken()));
					double totalD = 0.0;
						while(configVector.size() > 0)
						{
							String configElement = configVector.remove(0);
							String parent = configElement;
							String child = parent;
							
							// this element might be a composite // need to work this through
							double BIC = 0;
							if(configElement.indexOf("-") > 0)
							{
								// I need to process each one of these things but for now
								// I need to find the last key 
								// right now handling only the case of 2
								// could be more than 2 etc. 
								String [] parents = findParents(configElement, configVector);
								child = configElement.substring(configElement.indexOf("-") + 1);
								
								//System.out.println("Finding the path for... " + parents[0] + "   Through... " + parents[1] + "  to...  " + child);
								BIC = findBIC(parents[0], child, parents[1]);
								
								//totalBIC = totalBIC + BIC;
								
								//double parentD = 0.0; //findD(parent, null);
								double childD = findD(parents[0], child); // add the parent piece to it
								
								totalBIC = totalBIC + BIC ;
								totalD = totalD + childD; // removing the child d	
							}
							else
							{
								BIC = findBICI(parent); // add the parent as well
								double parentD = findD(parent, null);
								totalD = totalD + parentD;
								totalBIC = totalBIC + BIC;
							}
						} // completes a config right here
					// add the final penalty for this config
					double penalty = getLogN(columns[0], columns[1]) * (totalD/2);
					totalBIC = totalBIC - penalty;
					System.out.println("BIC for the pattern " + otherNodes.get(nodeIndex).leaf.getKey() + " >> " + totalBIC + "  Penalty..  " + penalty + "  D... " + totalD);
					BICIHash.put(otherNodes.get(nodeIndex).leaf.getKey(), totalBIC);
					// compare the BICs if this configuration is better.. then discard everything else and proceed for the next set of combos 
					// the next time when it comes in
					// there needs to be some kind of way to restart the process
					if(firstTime) // turn this into first one
					{
						baseBIC = totalBIC;
						curSelectedNode = otherNodes.get(nodeIndex);
						selectedNode = curSelectedNode; // I have no idea why I need it.. but.. 
						firstTime = false;
						continueToNext = true;
					}
					else if((totalBIC > baseBIC))
					{
						continueToNext = true;
						baseBIC = totalBIC;
						curSelectedNode = otherNodes.get(nodeIndex);
						selectedNode = curSelectedNode; // I have no idea why I need it.. but.. 
						System.err.println("Existing pattern.. new BIC " + selectedNode.leaf.getKey() + baseBIC);
						System.out.println("Pattern selected " + selectedNode.leaf.getKey());
						// set the selected node if this is not continue to next
						/*if(continueToNext)
						{
							selectedNode = curSelectedNode;
							break;
						}*/
					}
					/*
					 * this is closing it too prematurely.. I dont want to stop until we get another one
					else if(!skip)
					{
						// we have the winner 
						// stop
						System.out.println("Got here");
						break;
					}*/
				}catch(Exception ex)
				{
					System.err.println("Ok.. it did not work for this pattern.. " + otherNodes.get(nodeIndex).leaf.getKey());
					ex.printStackTrace();
				}
			}// ends the if something is already done piece
			else
			{
				//System.err.println("Ok.. we already have this.. " + otherNodes.get(nodeIndex).leaf.getKey());
				// if the bic is valid take it
				double thisBIC = BICIHash.get(otherNodes.get(nodeIndex).leaf.getKey());
				
				if(thisBIC >= baseBIC)
				{
					//System.out.println("This bic is higher.. so I am going change to this.. ");
					this.selectedNode = otherNodes.get(nodeIndex); // set this as the new one
					baseBIC = thisBIC;
				}				
			}
		}
		// do one last check to see if the input 
		//if(selectedNode.leaf.getKey().equalsIgnoreCase(curSelectedNode.leaf.getKey()))
		//	continueToNext = false;
		return continueToNext;
	}
	
	public String[] findParents(String pattern, Vector <String> configVector)
	{
		String [] retString = new String[2];
		String child = pattern.substring(pattern.indexOf("-") + 1); // gets the child pattern
		String parent = pattern.substring(0, pattern.indexOf("-"));
		retString[0] = parent;
		// for all the config elements find the parent
		String otherParents = ""; // + parent;
		for(int config = 0;config < configVector.size();config++)
		{
			String thisConfig = configVector.elementAt(config);
			if(thisConfig.indexOf("-" + child) >= 0)
			{
				otherParents = otherParents + ";" + thisConfig.substring(0,thisConfig.indexOf("-"));
				configVector.remove(config);
			}
		}
		if(otherParents.length() > 0)
		{
			otherParents = otherParents + ";" + parent + ";";
			retString[1] = otherParents;
		}
		else
			retString[1] = null;
		return retString;
	}
	
	
	public String[] findParent(String child)
	{
		// need to get the number of nodes that are parent and give back
		String SPARQL = "SELECT ?myNode WHERE {{?myNode rel:child " + child + " }}";
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, SPARQL);
		
		String [] variables = wrapper.getVariables();
		ArrayList <String> output = new ArrayList<String>();
		while(wrapper.hasNext())
		{
			ISelectStatement stmt = wrapper.next();
			String var = ""+stmt.getVar(variables[0]);
			var.replace("temp:", "");
			output.add(var);
		}
		
		return (String[]) output.toArray();
	}
	
	
	public int getOccurence(String colName)
	{
		int retOccurence = 0;
		if(occurenceHash.containsKey(colName))
			retOccurence = occurenceHash.get(colName);
		else
		{
			retOccurence = df.getUniqueInstanceCount(colName);
			occurenceHash.put(colName, retOccurence);
		}
		return retOccurence;
	}
	
	public int findParentConfig(String pattern)
	{
		// three things
		// if there is a -then take the previous one
		int retConfig = 1;
		if(pattern.indexOf("-") > 0)
		{
			int parIndex = Integer.parseInt(pattern.substring(0, pattern.indexOf("-") -1));
			retConfig = getOccurence(columns[parIndex]);
		}
		
		return retConfig;
	}
	
	/*public void findNet()
	{
		// first calculate the individuals
		String [] columns = df.levelNames;
		double baseBIC = 0.0;
		double residue = 0.0;
		
		
		
		for(int colIndex = 0;colIndex < columns.length;colIndex++)
		{
			double thisBICI = findBICI(columns[colIndex]);
			baseBIC = baseBIC + thisBICI;
			BICIHash.put(columns[colIndex], thisBICI);
			
			
			baseRi = baseRi * (df.getUniqueInstanceCount(columns[colIndex]) -1);
		}
		
		Hashtable <String, Hashtable<String,Integer>> childs = df.simpleTree.getPath(columns[0], columns[1]);
		
		int totalRows = 0;
		Enumeration <String> keys = childs.keys();
		while(keys.hasMoreElements())
		{
			Iterator <Integer> it = childs.get(keys.nextElement()).values().iterator();
			while(it.hasNext())
				totalRows = totalRows + it.next();
			
		}

		
		
		double penalty = (Math.log(totalRows) / Math.log(2))*(baseRi / 2.0);
		
		baseBIC = baseBIC - penalty;
		// base BIC is X1, X2, X3 separately.. 
		
		System.out.println("Base Expectation " + baseBIC);
		// start the trials now
		
		// start by manipulating the first column
		int holder = 0;
		int edgeMover = 1;
		
		
		while(holder < columns.length)
		{
			while(edgeMover < columns.length && !takenHash.containsKey(columns[edgeMover]))
			{
				
				System.out.println("BIC RIGHT NOW " + baseBIC);
				
				double thisBIC = 0.0; //findBIC(columns[holder], columns[edgeMover]);
				
				
				//takenHash.put(columns[holder], "true");
				takenHash.put(columns[edgeMover], "true");
	
				double remainingBIC = findRemainingBIC();
				
				if((thisBIC + remainingBIC + residue) > baseBIC) // boom killed it
				{
					baseBIC = thisBIC + remainingBIC + residue;
					residue = thisBIC;
					System.out.println("New Base BIC " + columns[holder] + " - > " + columns[edgeMover] + baseBIC);
					fromToHash.put(columns[holder], columns[edgeMover]);
					//break;
				}
				else
				{
					//takenHash.remove(columns[edgeMover]);
					takenHash.remove(columns[edgeMover]);
				}
				
				edgeMover++;
			}
			holder++;
		}
	}
	
	
	public double findRemainingBIC()
	{
		double finalValue = 0.0;
		
		Enumeration <String> keys = BICIHash.keys();
		while(keys.hasMoreElements())
		{
			String key = keys.nextElement();
			if(!takenHash.containsKey(key))
				finalValue = finalValue + BICIHash.get(key);
		}
		
		return finalValue;
	}*/
	
	
	
	
	public double findBICI(String child)
	{
		double expectation = 0.0;
		if(BICIHash.containsKey(child))
			expectation = BICIHash.get(child);
		else
		{
			Hashtable <String, Integer> childs = df.simpleTree.getNodeConfig(child);
			
			expectation = calculateExpectation(childs);
			
			// calculate penalty
			
			//System.out.println("Expectation..  " + child + " ... " + expectation);
		}
		
		return expectation;
	}
	
	public double findD(String parent, String child)
	{		
		int parentCount = getOccurence(parent);
		int N = parentCount;
		int parentD = (parentCount - 1);
		double totalD = parentD;
		
		// I need to account to see if there is a pipe on it
		// it could be multiple child
		if(child != null)
		{
			int childCount = getOccurence(child);
			N = N * childCount;
			int childD = (childCount - 1) * parentCount;
			totalD = childD; // modifying based on george's new logic
		}	
		return totalD;
	}
	
	public double findBIC(String parent, String child, String path)
	{
		double BIC = 0;
		String pattern = parent + "-" + child;
		if(BICIHash.containsKey(pattern))
			BIC = BICIHash.get(pattern);
		else
		{
			// get the column counts
			int numParent, numChild = 0;
			if(occurenceHash.containsKey(parent))
				numParent = occurenceHash.get(parent);
			else
			{
				numParent = df.getUniqueInstanceCount(parent);
				occurenceHash.put(parent, numParent);
			}
			
			if(occurenceHash.containsKey(child))
				numChild = occurenceHash.get(child);
			else
			{
				numChild = df.getUniqueInstanceCount(child);
				occurenceHash.put(child, numChild);
			}
			
			// next get the mix up
			Hashtable <String, Hashtable<String,Integer>> childs = df.simpleTree.getPath(parent, child, path);
			
			// next get the unique nodes for the parent
			Object [] parentInstances = df.getUniqueValues(parent);
			Object [] childInstances = df.getUniqueValues(child);
			
			// I need to find cartesion in terms of parents
			// calculate the expectation
			double totalLog = 0.0;
			Enumeration <String> childKeys = childs.keys();
			while(childKeys.hasMoreElements())
			//for(int parentIndex = 0;parentIndex < parentInstances.length;parentIndex++)
				totalLog = totalLog + getCrossTab(childs, childInstances, childKeys.nextElement()); //parentInstances[parentIndex]+"");
			

			/*
			//penalties - Obviously I assume that the numbers are not zero
			double childPenalty = Math.log(numParent * numChild)/Math.log(2);
			
			// calculate the qi
			double parentQi = 1.0; // I need to keep state so I can find if there is an existing piece here however.. 
			double childQi = numParent;
			
			// calculate the ds
			double parentD = (numParent - 1) * parentQi;
			double childD = (numChild - 1) * childQi;
			
			double totalD = (parentD + childD);
			
			BIC = totalLog - ((childPenalty * totalD) / 2);*/
			
			BIC = totalLog;
			
			//System.out.println("BIC is " + BIC);
		}
		
		return BIC;
		
	}
	
	public double getLogN(String parent, String child)
	{
		if(logN == 0.0)
		{
			Hashtable <String, Hashtable<String,Integer>> childs = df.simpleTree.getPath(parent, child);
				Enumeration <String> keys = childs.keys();
				while(keys.hasMoreElements())
				{
					Iterator <Integer> it = childs.get(keys.nextElement()).values().iterator();
					while(it.hasNext())
						totalRows = totalRows + it.next();
				}		
				
				logN = Math.log(totalRows) / Math.log(2);
		}
		return logN;

	}
	
	public double getCrossTab(Hashtable <String, Hashtable<String, Integer>> childs, Object [] childInstances, String id)
	{
		Hashtable <String, Integer> retTable = new Hashtable<String, Integer>();
		// get to the id first
		Hashtable <String, Integer> theseInstances = childs.get(id);
		
		// now I have the instances
		int total = 0;
		total = getTotal(childInstances, theseInstances);
		//System.out.println("Node ...    " + total);
		theseInstances.put("total", total);
		return calculateExpectation(theseInstances);
	}
	
	public int getTotal(Object [] childInstances, Hashtable <String, Integer> theseInstances)
	{
		int total = 0;
		for(int childIndex = 0;childIndex < childInstances.length;childIndex++)
		{
			
			String child = childInstances[childIndex] +"";
			if(theseInstances.containsKey(child))
			{
				total = total + theseInstances.get(child);
				//SimpleTreeNode childNode = theseInstances.elementAt(scIndex);
				//if((childNode.leaf.getValue() + "").equalsIgnoreCase(child))
				//	count++;
			}
			else
			{
				theseInstances.put(child, 0);
			}
		}
		
		return total;
	}
	
	
	public double calculateExpectation(Hashtable <String, Integer> inputHash)
	{
		// calculates the i value
		double retFloat = (long) 0.0;
		// input hash has various things
		double total = inputHash.get("total");
		inputHash.remove("total");
		
		Object [] values = (Object [])inputHash.values().toArray();
		
		for(int valIndex = 0;valIndex < values.length;valIndex++)
		{
			double val = ((Integer)values[valIndex]).doubleValue();
			
			if(val != 0 && total != 0)
			{
				double logged = val / total;
				
				double logVal = Math.log(logged) / Math.log(2);
				
				retFloat = retFloat + val * logVal;
			}
		}
		
		//System.out.println("Out put.....  " + retFloat);
		
		return retFloat;
	}
	
	
	
	
	
	
	

}
