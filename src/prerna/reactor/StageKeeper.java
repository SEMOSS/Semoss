/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class StageKeeper {

  // main class for keeping all the stages
  // keeps the operation to what stage the operation is in
  // StageKeeper is to stage what Stage is to operations

  // actually it is lot more simple
  // I need to see what operation sequence

  Vector<Stage> stageSequence = new Vector<Stage>();
  Hashtable<String, Stage> stageHash = new Hashtable<String, Stage>();
  Stage lastStage = null;

  // needs to have a dependency
  Hashtable<String, Vector<String>> stageDependencies = new Hashtable<String, Vector<String>>();

  public void addStage(String operationName, Stage stage) {
    stageHash.put(operationName, stage);
    if (stageSequence.indexOf(stage) < 0) stageSequence.insertElementAt(stage, 0);
    lastStage = stage;
  }

  public void adjustStages() {
    // see if the stage has repeated
    // if so.. discard the last one and keep the first one
    Vector<Stage> newSequence = new Vector<Stage>();
    int stageReset = 1;
    for (int stageIndex = 0; stageIndex < stageSequence.size(); stageIndex++) {
      if (newSequence.indexOf(stageSequence.elementAt(stageIndex)) < 0) {
        stageSequence.elementAt(stageIndex).stageNum = stageReset;
        newSequence.add(stageSequence.elementAt(stageIndex));
        stageReset++;
      }
    }
    // reset it
    stageSequence = newSequence;
  }

  public Iterator processStages() {
    // I need to process every stage
    // until I get to the last one
    // at which point I should just return the iterator
    // as in the lambda
    System.out.println("Total Stages.. " + stageSequence.size());
    Stage lastStage = stageSequence.lastElement();
    stageSequence.remove(lastStage);
    Hashtable<String, Object> stageStore = null;
    for (int stageIndex = 0; stageIndex < stageSequence.size(); stageIndex++) {
      Stage thisStage = stageSequence.elementAt(stageIndex);
      if (stageStore != null) thisStage.addStore(stageStore);
      thisStage.preProcessStage();
      thisStage.processStage();
      stageStore = thisStage.postProcessStage();
    }
    if (stageStore != null) lastStage.addStore(stageStore);
    lastStage.preProcessStage();
    return lastStage.runner;
  }

  public void printCode() {
    for (int stageIndex = 0; stageIndex < stageSequence.size(); stageIndex++) {
      Stage thisStage = stageSequence.elementAt(stageIndex);
      System.out.println(thisStage.getCode());
    }
  }
}
