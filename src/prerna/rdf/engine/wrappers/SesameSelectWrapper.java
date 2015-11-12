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
package prerna.rdf.engine.wrappers;

import java.util.List;

import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.BTreeDataFrame;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.util.Constants;
import prerna.util.Utility;

public class SesameSelectWrapper extends AbstractWrapper implements ISelectWrapper {

	public transient TupleQueryResult tqr = null;

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		tqr = (TupleQueryResult) engine.execQuery(query);
	}

	@Override
	public boolean hasNext() {
		boolean retBool = false;
		try {
			retBool = tqr.hasNext();
			if (!retBool)
				tqr.close();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retBool;
	}

	@Override
	public ISelectStatement next() {
		// TODO Auto-generated method stub
		ISelectStatement sjss = null;
		// thisSt = new SesameJenaSelectStatement();
		try {
			logger.debug("Adding a sesame statement ");
			BindingSet bs = tqr.next();
			sjss = getSelectFromBinding(bs);
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sjss;
	}

	protected ISelectStatement getSelectFromBinding(BindingSet bs)
	{
	
		String [] variableArr = displayVar;
		/*if(displayVar != null){
			variableArr = displayVar;
		} else {
			variableArr =var;
		}*/
		
		ISelectStatement sjss = new SelectStatement();
		for(int colIndex = 0;colIndex < variableArr.length;colIndex++)
		{
			Object val = bs.getValue(var[colIndex]);
			Object parsedVal = getRealValue(val);

			sjss.setVar(variableArr[colIndex], parsedVal);
			if(val!=null){
				sjss.setRawVar(variableArr[colIndex], val);
			}
			logger.debug("Binding Name " + variableArr[colIndex]);
		}
		return sjss;
	}
	
	private Object getRealValue(Object val){
		try
		{
			if(val != null && val instanceof Literal)
			{
				if(QueryEvaluationUtil.isStringLiteral((Value) val)){
					return ((Literal)val).getLabel();
				}
				else if((val.toString()).contains("http://www.w3.org/2001/XMLSchema#dateTime")){
					return (val.toString()).substring((val.toString()).indexOf("\"")+1, (val.toString()).lastIndexOf("\""));
				}
				else{
					logger.debug("This is a literal impl >>>>>> "  + ((Literal)val).doubleValue());
					return new Double(((Literal)val).doubleValue());
				}
			}else if(val != null && val instanceof com.hp.hpl.jena.rdf.model.Literal)
			{
				logger.debug("Class is " + val.getClass());
				return new Double(((Literal)val).doubleValue());
			}
			if(val!=null){
				String value = val+"";
				return Utility.getInstanceName(value);
			}
		}catch(RuntimeException ex)
		{
			logger.debug(ex);
		}
		return "";
	}

	@Override
	public String[] getVariables() {
		return getDisplayVariables();
	}
	
	@Override
	public String[] getDisplayVariables() {
		if(displayVar == null){
			try {

				displayVar = new String[tqr.getBindingNames().size()];
				List<String> names = tqr.getBindingNames();
				for (int colIndex = 0; colIndex < names.size(); colIndex++){
					String columnLabel = names.get(colIndex);
					String tableLabel = names.get(colIndex);
					boolean columnIsProperty = false;
					String tableLabelURI = Constants.CONCEPT_URI;
					String columnLabelURI = Constants.RELATION_URI;
					if(columnLabel.contains("__")){
						columnIsProperty = true;
						String[] splitColAndTable = columnLabel.split("__");
						tableLabel = splitColAndTable[0];
						columnLabel = splitColAndTable[1];
					}
				
					tableLabelURI += tableLabel;
					columnLabelURI += columnLabel;
					//now get the display name 
					tableLabelURI = engine.getTransformedNodeName(tableLabelURI, true);
					columnLabelURI = engine.getTransformedNodeName(columnLabelURI, true);
					tableLabel = Utility.getInstanceName(tableLabelURI);
					columnLabel = Utility.getInstanceName(columnLabelURI);
					if(columnIsProperty){
						columnLabel = tableLabel + "__" + columnLabel;
					} else {
						columnLabel = tableLabel;
					}
					
					displayVar[colIndex] = columnLabel;
				}
					
			} catch (QueryEvaluationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return displayVar;
	}
	
	@Override
	public String[] getPhysicalVariables() {
		if(var == null){
			try {
				var = new String[tqr.getBindingNames().size()];
				List<String> names = tqr.getBindingNames();
				for (int colIndex = 0; colIndex < names.size(); var[colIndex] = names
						.get(colIndex), colIndex++)
					;
			} catch (QueryEvaluationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return var;
	}

	@Override
	public ITableDataFrame getTableDataFrame() {
		BTreeDataFrame dataFrame = new BTreeDataFrame(this.displayVar);
		while (hasNext()){
			try {
				logger.debug("Adding a sesame statement ");
				BindingSet bs = tqr.next();
				Object[] clean = new Object[this.displayVar.length];
				Object[] raw = new Object[this.displayVar.length];
				for(int colIndex = 0;colIndex < displayVar.length;colIndex++)
				{
					raw[colIndex] = bs.getValue(var[colIndex]);
					clean[colIndex] = getRealValue(raw[colIndex]);
				}
				dataFrame.addRow(clean, raw);
			} catch (QueryEvaluationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return dataFrame;
	}
}
