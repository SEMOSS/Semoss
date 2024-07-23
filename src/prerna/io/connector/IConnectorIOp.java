package prerna.io.connector;

import java.util.Map;

import prerna.auth.User;

public interface IConnectorIOp {
	
	// basic operations the users do
	
	// profile
	
	// get a list of files or items
	
	// select something from the list
	
	// get content of the selected item from the list
	
	// create a new item with content
	
	// update the content of the item
	
	// delete the content of the item 
	
	public Object execute(User user, Map<String, Object> params);

}
