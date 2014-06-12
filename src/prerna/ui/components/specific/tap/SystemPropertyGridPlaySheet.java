package prerna.ui.components.specific.tap;

import prerna.ui.components.playsheets.GridPlaySheet;

public class SystemPropertyGridPlaySheet  extends GridPlaySheet {

	/**
	 * Sets the string version of the SPARQL query on the playsheet. 
	 * @param query String
	 */
	@Override
	public void setQuery(String query) 
	{
		if(query.startsWith("SELECT")||query.startsWith("CONSTRUCT"))
			this.query=query;
		else
		{
			logger.info("Query " + query);

			String addConditions = "";
			String addBindings = "";

			int semicolon1 = query.indexOf(";");
			int semicolon2 = query.indexOf(";",semicolon1+1);
			int semicolon3 = query.indexOf(" SELECT",semicolon2+1);

			String probabilityUserResponse = query.substring(0,semicolon1);
			String interfaceUserResponse = query.substring(semicolon1+1,semicolon2);
			String archiveUserResponse = query.substring(semicolon2+1, semicolon3);
			query = query.substring(semicolon3+1);

			if(probabilityUserResponse.equals("All"))
			{
				// do nothing
			} else {
				addConditions = addConditions.concat("{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} ");
				if(probabilityUserResponse.equals("High"))
				{
					addBindings = addBindings.concat("BINDINGS ?Probability {('High')('Question')}");
				} else {
					addBindings = addBindings.concat("BINDINGS ?Probability {('Low')('Medium')('Medium-High')}");
				}
			}


			if(interfaceUserResponse.equals("All"))
			{
				// do nothing
			} else if(interfaceUserResponse.equals("Yes"))
			{
				addConditions = addConditions.concat("{?System <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y' }");
			} else if(interfaceUserResponse.equals("No"))
			{
				addConditions = addConditions.concat("{?System <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'N' }");
			}

			if(archiveUserResponse.equals("All"))
			{
				// do nothing
			} else if(archiveUserResponse.equals("Yes"))
			{
				addConditions = addConditions.concat("{?System <http://semoss.org/ontologies/Relation/Contains/Archive_Req> 'Y' }");
			} else if(archiveUserResponse.equals("No"))
			{
				addConditions = addConditions.concat("{?System <http://semoss.org/ontologies/Relation/Contains/Archive_Req> 'N' }");
			}

			String retString = "";
			retString = retString.concat(query.substring(0,query.lastIndexOf("}"))).concat(addConditions).concat("} ").concat("ORDER BY ?System ").concat(addBindings);
			logger.info("New Query " + retString);
			this.query = retString;
		}
	}
}
