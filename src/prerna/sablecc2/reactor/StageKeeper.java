package prerna.sablecc2.reactor;

import java.util.Hashtable;
import java.util.Vector;

public class StageKeeper {
	
	// main class for keeping all the stages
	// keeps the operation to what stage the operation is in
	// StageKeeper is to stage what Stage is to operations
	
	// actually it is lot more simple
	// I need to see what operation sequence
	
	Vector <Stage> stageSequence = new Vector<Stage>();
	Hashtable <String, Stage> stageHash = new Hashtable <String, Stage>();
	Stage lastStage = null;
	
	// needs to have a dependency
	Hashtable <String, Vector<String>> stageDependencies = new Hashtable <String, Vector<String>>();
	
	public void addStage(String operationName, Stage stage)
	{
		stageHash.put(operationName, stage);
		if(stageSequence.indexOf(stage) < 0)
			stageSequence.insertElementAt(stage,0);
		lastStage = stage;
	}
	
	public void adjustStages()
	{
		// see if the stage has repeated
		// if so.. discard the last one and keep the first one
		Vector <Stage> newSequence = new Vector<Stage>();
		int stageReset = 1;
		for(int stageIndex = 0;stageIndex < stageSequence.size();stageIndex++)
		{
			if(newSequence.indexOf(stageSequence.elementAt(stageIndex)) < 0)
			{
				stageSequence.elementAt(stageIndex).stageNum = stageReset;
				newSequence.add(stageSequence.elementAt(stageIndex));
				stageReset++;
			}
		}
		// reset it
		stageSequence = newSequence;
	}
	
	public void printCode()
	{
		for(int stageIndex = 0;stageIndex < stageSequence.size();stageIndex++)
		{
			Stage thisStage = stageSequence.elementAt(stageIndex);
			System.out.println(thisStage.getCode());
		}		
	}

}
