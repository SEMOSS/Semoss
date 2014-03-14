package prerna.ui.components.specific.tap;

import java.util.ArrayList;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.GridPlaySheet;

public class DeduplicationQueriesPlaySheet extends GridPlaySheet{

	@Override
	public void createData() {
		// TODO Auto-generated method stub
		// the create view needs to refactored to this
		String[] queryList = query.split("@:@");
		list = new ArrayList<Object[]>();
		list = runQuery(queryList[0]);
		list.addAll(addAdditionalQuery(queryList[1], list));
	}

	public ArrayList<Object[]> runQuery(String query)
	{
		ArrayList<Object[]> addList = new ArrayList<Object[]>();
		wrapper = new SesameJenaSelectWrapper();
		if(engine!= null && rs == null){
			wrapper.setQuery(query);
			wrapper.setEngine(engine);
			try{
				wrapper.executeQuery();	
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}		

		}
		else if (engine==null && rs!=null){
			wrapper.setResultSet(rs);
			wrapper.setEngineType(IEngine.ENGINE_TYPE.JENA);
		}

		// get the bindings from it
		names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();

				Object [] values = new Object[names.length];
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					values[colIndex] = getVariable(names[colIndex], sjss);
					logger.debug("Binding Name " + names[colIndex]);
					logger.debug("Binding Value " + values[colIndex]);
				}
				logger.debug("Creating new Value " + values);
				addList.add(values);
			}
		} catch (Exception e) {
			logger.fatal(e);
		}

		return addList;
	}

	public ArrayList<Object[]> addAdditionalQuery(String query, ArrayList<Object[]> list)
	{
		ArrayList<Object[]> addList = new ArrayList<Object[]>();
		wrapper = new SesameJenaSelectWrapper();
		if(engine!= null && rs == null){
			wrapper.setQuery(query);
			wrapper.setEngine(engine);
			try{
				wrapper.executeQuery();	
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}		

		}
		else if (engine==null && rs!=null){
			wrapper.setResultSet(rs);
			wrapper.setEngineType(IEngine.ENGINE_TYPE.JENA);
		}

		// get the bindings from it
		names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();

				Object [] values = new Object[names.length];
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					values[colIndex] = getVariable(names[colIndex], sjss);
					logger.debug("Binding Name " + names[colIndex]);
					logger.debug("Binding Value " + values[colIndex]);
				}

				if(list.contains(values))
				{
					System.out.println(list.get(0) + "::::" + list.get(0).getClass());
					System.out.println(values[0] + ":::" + values.getClass());
					logger.debug("Creating new Value " + values);
					addList.add(values);
				}
			}
		} catch (Exception e) {
			logger.fatal(e);
		}

		return addList;
	}
}
