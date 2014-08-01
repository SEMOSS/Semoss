/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
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
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;

import org.apache.log4j.Logger;

import prerna.ui.helpers.EntityFiller;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This provides a panel to interact with various parameters.
 */
public class ParamPanel extends JPanel implements ActionListener {
	
	Logger logger = Logger.getLogger(getClass());

	Hashtable<String, String> params = null;
	Hashtable<String, String> paramType = null;
	String questionId = "";
	Hashtable knownValues = new Hashtable();
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
		
		Enumeration<String> keys = params.keys();
		int elementInt=0;
		ArrayList<String> setParams = new ArrayList<String>();
		
		//Iterate over params retrieved from query
		while(keys.hasMoreElements())
		{
			String key = (String)keys.nextElement();
			JLabel label = new JLabel(key);
			label.setFont(new Font("Tahoma", Font.PLAIN, 12));
			label.setForeground(Color.DARK_GRAY);
			
			//Execute the logic for filling the information here
			String entityType = (String)paramType.get(key);
			String [] fetching = {"Fetching"};
			
			ParamComboBox field = new ParamComboBox(fetching);
			field.setFont(new Font("Tahoma", Font.PLAIN, 11));
			field.setParamName(key);
			field.setEditable(false);
			field.setPreferredSize(new Dimension(100, 25));
			field.setMinimumSize(new Dimension(100, 25));
			//field.setMaximumSize(new Dimension(200, 32767));
			field.setBackground(new Color(119, 136, 153)); //Dropdown background color

			// get the list
			JList<String> list = (JList<String>) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
			// get the selected repository
			selectedEngines = list.getSelectedValuesList();
			String query = "";
			
			//Execute logic to set Entity Filler up for dependency
			//try to get the query associated with the label
			
			if(DIHelper.getInstance().getProperty(this.questionId + "_" + key + "_" + Constants.QUERY) != null)
				query = DIHelper.getInstance().getProperty(this.questionId + "_" + key + "_" + Constants.QUERY);
			
			//see if the query needs to be filled
			//if it does, set it as a dependent box
			if(checkIfFullQuery(query))
			{
				for(String engine : selectedEngines)
				{
					EntityFiller filler = new EntityFiller();
					filler.engineName = engine;
					filler.box = field;
					filler.type = entityType;
					if(query != null && !query.isEmpty()) {
						filler.extQuery = query;
					}
					Thread aThread = new Thread(filler);
					aThread.start();
				}
				setSelected(field);
				setParams.add(field.fieldName);
				field.addActionListener(this);
			}
			else
			{
				setDependencies(field, query);
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
						}
						Thread aThread = new Thread(filler);
						aThread.start();
					}
					setSelected(field);
					newChangedParams.add(field.fieldName);
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
		list.add(source.fieldName);
		fillParams(list);
	}
	
	/**
	 * Sets selected value into the parameter combo box.
	 * @param source 	
	 */
	private void setSelected(ParamComboBox source){
		String selectedValue = source.getURI(source.getSelectedItem().toString());
		if(selectedValue!=null)
			knownValues.put(source.getParamName(), selectedValue);
	}
	
	/**
	 * Sets parameters.
	 * @param params Hashtable of parameters.
	 */
	public void setParams(Hashtable<String, String> params)
	{
		this.params = params;
	}

	/**
	 * Sets the type of parameter.
	 * @param paramType 	Hashtable of parameter types.
	 */
	public void setParamType(Hashtable<String, String> paramType)
	{
		this.paramType = paramType;
	}

	/**
	 * Puts the parameter names and values into the parameters hashtable.
	 * @param paramName 	Parameter name.
	 * @param paramValue 	Parameter value.
	 */
	public void setParam(String paramName, String paramValue)
	{
		params.put(paramName, paramValue);
	}

	/**
	 * Gets parameters.
	
	 * @return Hashtable<String,String>	Hashtable of parameters. */
	public Hashtable<String, String> getParams()
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
	private void setDependencies(ParamComboBox field, String query)
	{
		ArrayList<String> dependencies = new ArrayList<String>();
		Pattern pattern = Pattern.compile("[@]\\w+[@]");
		Matcher matcher = pattern.matcher(query);
		while(matcher.find()){
			String data = matcher.group();
			data = data.substring(1,data.length()-1);
			logger.info(data);
			dependencies.add(data);
		}
		field.setDependency(dependencies);
		field.setQuery(query);
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
