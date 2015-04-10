package prerna.ds;

import java.util.concurrent.Phaser;

public class TreeThreader implements Runnable {

	SimpleTreeBuilder builder = null; // will be set at the constructor
	ISEMOSSNode [] parents = null;
	String [] childTypeToGet = null;
	Phaser phaser = null; // this is the semaphore
	ILinkeableEngine engine = null;
	
	public TreeThreader(SimpleTreeBuilder builder, Phaser phaser)
	{
		this.builder = builder;
		this.phaser = phaser;
	}
	
	// parents
	public void setParents(ISEMOSSNode [] parents)
	{
		this.parents = parents;
	}
	
	public void setChildTypes(String [] childTypeToGet)
	{
		this.childTypeToGet = childTypeToGet;
	}
	
	public void setEngine(ILinkeableEngine engine)
	{
		this.engine = engine;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		// The job of the Tree Threader is to construct the tree in a threaded fashion
		// Threaded tree typically gets
		// a. List of parent Nodes
		// b. the database to use to pick the child nodes from
		// c. Copy of the simple tree builder to make sure it is building everything of it
		// d. a Phaser so that it can stay in lock step with other fellow tree threader
		// e. Number of levels to build out --== Not sure how I would build out multi level ==--
		// The tree threader starts by interrogating the database for the child nodes
		// registers to the phaser
		// This it does by first finding from redis if something has been cached
		// Once it gets the data
		// the data is of the form of SEMOSS Nodes which it can add see ILinkeableEngine for details
		// Now comes the part of building the tree
		// at this point the Simple Tree Builder reference is there
		// which it uses to build tree
		// I need something in the tree builder which tells me the number of nodes at the given level
		// Once it finishes the current level
		// the tree builder will get the next set, filter it and then build again
		// Once the phase is complete, the treeThreader will just arrive and await advance
		// The simple tree builder in the interim will set the parents again and reinitiate
		// this continues until all the levels are complete
		
		// I am absolutely amazed at how surprisingly simple this is
		// wow..
		
		phaser.register();
		while(!phaser.isTerminated())
		{
			// get the child
			// getting only one right now
			System.out.println(".");
			ISEMOSSNode [][] nodes = engine.getRelatedNodes(parents, childTypeToGet[0]);
			System.out.println("." + parents + nodes);
			int rows = nodes.length;
			for(int rowIndex = 0;rowIndex <  rows;rowIndex++)
			{
				ISEMOSSNode [] oneRow = nodes[rowIndex];
				int columns = oneRow.length;
				for(int columnIndex = 0;columnIndex < columns;columnIndex++)
				{
					//System.out.println("Adding.. " +  Thread.currentThread() + parents[rowIndex].getKey() + "::" + parents[rowIndex].getType() + nodes[rowIndex][columnIndex].getType() + "::" + nodes[rowIndex][columnIndex].getKey());
					builder.addNode(parents[rowIndex], nodes[rowIndex][columnIndex]);
					//System.out.println("Added.. ");
				}
			}
			// once I am done with it.. just wait
			System.err.println("Waiting Now " + Thread.currentThread());
			phaser.arriveAndAwaitAdvance();
		}
		System.out.println("Comes to an end... dan dan dan.. ");
	}
}
