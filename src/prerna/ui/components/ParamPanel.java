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
package prerna.ui.components;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.SEMOSSParam;
import prerna.ui.helpers.EntityFiller;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This provides a panel to interact with various parameters.
 */
public class ParamPanel extends JPanel implements ActionListener {
	
	static final Logger logger = LogManager.getLogger(ParamPanel.class.getName());

	//Hashtable<String, String> params = null;
	List<SEMOSSParam> params = null;
	//Hashtable<String, String> paramType = null;
	String questionId = "";
	Map<String, List<Object>> knownValues = new Hashtable<String, List<Object>>();
	ArrayList<ParamComboBox> dependentBoxes = new ArrayList();
	List<String> selectedEngines;
	
	/**
	 * Constructor for ParamPanel.
	 */
	public ParamPanel() {
		setBackground(Color.WHITE);
	}

	/**
	 * Paints the parameter panel.
	 */
	public void paintParam()
	{
		//Remove all components (combo boxes) before repainting
		this.removeAll();
		
		// this is a QUICK FIX needs to be revisited
		// this is a quick fix and needs to be resolved
		// key is like this TAP_Core_Data:Generic-Perspective:GQ6 - because of the new scheme
		StringTokenizer tokenz = new StringTokenizer(questionId, ":");
		String newId = null;
		while(tokenz.hasMoreTokens())
			newId = tokenz.nextToken();
		questionId = newId;
		
		//Set up layout values for panel
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWidths = new int[] {0,0};
		gridBagLayout.rowHeights = new int[] {0, 0, 0};
		gridBagLayout.columnWeights = new double[]{0.0, 1.0};
		gridBagLayout.rowWeights = new double[]{0.0, 0.0};
		this.setLayout(gridBagLayout);

		ArrayList<ParamComboBox> fields = new ArrayList<ParamComboBox>();
		ArrayList<GridBagConstraints> gbcs1 = new ArrayList<GridBagConstraints>();
		ArrayList<GridBagConstraints> gbcs2 = new ArrayList<GridBagConstraints>();
		ArrayList<JLabel> labels = new ArrayList<JLabel>();
		GridBagConstraints gbc_element = new GridBagConstraints();
		
		//Enumeration<String> keys = params.keys();
		int elementInt=0;
		ArrayList<String> setParams = new ArrayList<String>();
		
		for(int i=0; i < params.size(); i++){
			SEMOSSParam param = params.get(i);
			String paramID = param.getParamID();
			String paramName = param.getName();
			String paramType = param.getType();
			String paramQuery = param.getQuery();
			boolean isParamQueryDb = param.isDbQuery();
			//should be a boolean; need to change SEMOSSParam's isDepends to boolean?
			String paramIsDepends = param.isDepends();
			
			JLabel label = new JLabel(paramName);
			label.setFont(new Font("Tahoma", Font.PLAIN, 12));
			label.setForeground(Color.DARK_GRAY);
			
			//Execute the logic for filling the information here
			String entityType = paramType;
			String [] fetching = {"Fetching"};
			
			ParamComboBox field = new ParamComboBox(fetching);
			field.setFont(new Font("Tahoma", Font.PLAIN, 11));
			field.setParamID(paramID);
			field.setParamName(paramName);
			field.setEditable(false);
			field.setPreferredSize(new Dimension(100, 25));
			field.setMinimumSize(new Dimension(100, 25));
			//field.setMaximumSize(new Dimension(200, 32767));
			field.setBackground(new Color(119, 136, 153)); //Dropdown background color
			System.err.println(params.get(i).getName());
			
			// get the list
			JList<String> list = (JList<String>) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
			// get the selected repository
			selectedEngines = list.getSelectedValuesList();
			
			//Note: fine up to here but query has @entity@ which needs to be replaced with the type before going to next step.
			
			//see if the query needs to be filled
			//if it does, set it as a dependent box
			if(paramIsDepends.equals("false"))
			{
				for(String engine : selectedEngines)
				{
					EntityFiller filler = new EntityFiller();
					filler.engineName = engine;
					filler.box = field;
					filler.type = entityType;
					filler.param = param;
					
					/*if(query != null && !query.isEmpty()) {
						filler.extQuery = query;
					}*/
					Thread aThread = new Thread(filler);
					aThread.start();
					
					synchronized(field) {
						try {
							field.wait();
						} catch (InterruptedException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				}
				setSelected(field);
				setParams.add(field.paramID);
				field.addActionListener(this);
			}
			else
			{
				setDependencies(field, param);
				field.type = entityType;
			}
			
			gbc_element = new GridBagConstraints();	
			gbc_element.anchor = GridBagConstraints.WEST;
			gbc_element.insets = new Insets(0, 5, 5, 5);
			gbc_element.gridx = 0;
			gbc_element.gridy = elementInt;
			labels.add(label);
			gbcs1.add(gbc_element);
			
			gbc_element = new GridBagConstraints();
			gbc_element.anchor = GridBagConstraints.NORTH;
			gbc_element.fill = GridBagConstraints.HORIZONTAL;
			gbc_element.insets = new Insets(0, 5, 5, 5);
			gbc_element.gridx = 1;
			gbc_element.gridy = elementInt;
			
			fields.add(field);
			gbcs2.add(gbc_element);
			elementInt++;
		}
		
		int index=0;
		int begAlph=0;
		while(fields.size()>1)
		{
			begAlph=0;
			for(int i=1;i<fields.size();i++)
			{
				if(fields.get(begAlph).getParamName().compareTo(fields.get(i).getParamName())>0)
					begAlph=i;
			}
			gbcs1.get(begAlph).gridy=index;
			gbcs2.get(begAlph).gridy=index;
			index++;
			this.add(labels.get(begAlph),gbcs1.get(begAlph));
			this.add(fields.get(begAlph),gbcs2.get(begAlph));
			fields.remove(begAlph);
			gbcs1.remove(begAlph);
			labels.remove(begAlph);
			gbcs2.remove(begAlph);
		}
		if(!gbcs1.isEmpty())
		{
			gbcs1.get(0).gridy=index;
			gbcs2.get(0).gridy=index;
			this.add(labels.get(0),gbcs1.get(0));
			this.add(fields.get(0),gbcs2.get(0));
		}
		fillParams(setParams);
	}
	
	/**
	 * Fills the list of parameters with newly changed parameters based on whether the list is full from a previous query.
	 * @param changedParams 	List of changed parameters to be added.
	 */
	private void fillParams(ArrayList<String> changedParams){
		ArrayList<String> newChangedParams = new ArrayList<String>();
		for(ParamComboBox field : dependentBoxes){
			//check if the box is dependent on the changed param
			HashSet<String> overlap = new HashSet<String>(field.dependency);
			overlap.retainAll(changedParams);
			if (!overlap.isEmpty()){
				String query = field.query;
				query = Utility.fillParam(query, knownValues);
				
				if(checkIfFullQuery(query))
				{
					String entityType = field.type;
					for(String engine : selectedEngines)
					{
						EntityFiller filler = new EntityFiller();
						filler.engineName = engine;
						filler.box = field;
						filler.type = entityType;
						if(query != null && !query.isEmpty()) {
							filler.extQuery = query;
							filler.extQueryUnBound = field.query; 
							filler.extQueryBindings.putAll(knownValues);
						}
						Thread aThread = new Thread(filler);
						aThread.start();
						
						synchronized(field) {
							try {
								while(field.getItemCount() == 0) {
									field.wait();
								}
							} catch (InterruptedException e) {
								logger.error(Constants.STACKTRACE, e);
							}
						}
					}
					setSelected(field);
					newChangedParams.add(field.paramID);
					field.addActionListener(this);
				}
			}
		}
		if(!newChangedParams.isEmpty())
			fillParams(newChangedParams);
	}
	
	/**
	 * Invoked when an action occurs.
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		ParamComboBox source = (ParamComboBox) arg0.getSource();
		setSelected(source);
		
		ArrayList list = new ArrayList();
		list.add(source.paramID);
		fillParams(list);
	}
	
	/**
	 * Sets selected value into the parameter combo box.
	 * @param source 	
	 */
	private void setSelected(ParamComboBox source){
		String selectedValue = source.getURI(source.getSelectedItem().toString());
		if(selectedValue!=null) {
			List<Object> selectValueList = new ArrayList<Object>();
			selectValueList.add(selectedValue);
			knownValues.put(source.getParamName(), selectValueList);
		}
	}
	
	/**
	 * Sets parameters.
	 * @param params Hashtable of parameters.
	 */
	/*public void setParams(Hashtable<String, String> params)
	{
		this.params = params;
	}*/
	
	public void setParams(List<SEMOSSParam> params)
	{
		this.params = params;
	}
	
	/**
	 * Sets the type of parameter.
	 * @param paramType 	Hashtable of parameter types.
	 */
	/*public void setParamType(Hashtable<String, String> paramType)
	{
		this.paramType = paramType;
	}*/

	/**
	 * Puts the parameter names and values into the parameters hashtable.
	 * @param paramName 	Parameter name.
	 * @param paramValue 	Parameter value.
	 */
	/*public void setParam(String paramName, String paramValue)
	{
		params.put(paramName, paramValue);
	}*/

	/**
	 * Gets parameters.
	
	 * @return Hashtable<String,String>	Hashtable of parameters. */
	public List<SEMOSSParam> getParams()
	{
		return params;
	}
	
	/**
	 * Sets the question ID.
	 * @param questionId 	Question ID.
	 */
	public void setQuestionId(String questionId) {
		this.questionId = questionId;
	}
	
	/**
	 * Gets the question ID.
	
	 * @return String	Question ID. */
	public String getQuestionId() {
		return this.questionId;
	}
	
	/**
	 * Sets a new question.
	 * @param newQuestion 	If true, clears the known values hashtable.
	 */
	public void setNewQuestion(boolean newQuestion) {
		if(newQuestion)
			knownValues.clear();
	}
	
	/**
	 * Gets data from the queries and adds to the list of dependencies.
	 * @param field 	Parameters combo box where dependencies and queries are set.
	 * @param query 	Query.
	 */
	private void setDependencies(ParamComboBox field, SEMOSSParam param)
	{
		Vector<String> dependencies = param.getDependVars();
		String query = param.getQuery();
		
		field.setDependency(dependencies);
		field.setQuery(query);
		field.setParamID(param.getParamID());
		this.dependentBoxes.add(field);
	}
	
	/**
	 * Checks if the query is in the list.
	 * @param query 		Query to be checked.
	
	 * @return boolean		True if the pattern is not found in the matcher.s */
	private boolean checkIfFullQuery(String query){
		boolean ret = true;
		Pattern pattern = Pattern.compile("[@]\\w+[@]");
		Matcher matcher = pattern.matcher(query);
		if(matcher.find())
			ret = false;
		return ret;
	}
}
