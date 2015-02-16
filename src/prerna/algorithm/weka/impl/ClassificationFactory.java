/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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
package prerna.algorithm.weka.impl;

import weka.classifiers.Classifier;
import weka.classifiers.functions.SimpleLogistic;
import weka.classifiers.rules.DecisionTable;
import weka.classifiers.rules.PART;
import weka.classifiers.trees.ADTree;
import weka.classifiers.trees.BFTree;
import weka.classifiers.trees.DecisionStump;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.J48graft;
import weka.classifiers.trees.LADTree;
import weka.classifiers.trees.LMT;
import weka.classifiers.trees.REPTree;
import weka.classifiers.trees.SimpleCart;

public final class ClassificationFactory {

	private ClassificationFactory() {
		
	}
	
	public static Classifier createClassifier(String type) {
		switch(type.toUpperCase()) {
		//tree outputs
		case "J48" : return new J48();
		case "J48GRAFT" : return new J48graft();
		case "SIMPLECART" : return new SimpleCart();
		case "REPTREE" : return new REPTree();
		case "BFTREE" : return new BFTree();
		// probability based
		case "ADTREE" : return new ADTree();
		case "LADTREE" : return new LADTree();
		//rule outputs
		case "PART" : return new PART();
		case "DECISIONTABLE" : return new DecisionTable();
		case "DECISIONSTUMP" : return new DecisionStump();
		case "LMT" : return new LMT();
		case "SIMPLELOGISTIC" : return new SimpleLogistic();
		}

		return null;
	}
	
}
