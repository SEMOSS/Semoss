package prerna.reactor.workflowtool;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import java.util.HashMap;
import java.util.Calendar;
import java.util.Date;

public class ItemReactor extends AbstractReactor{
	
	public ItemReactor() {
		// Setting up the keys so their names are adjacent to the attributes in the database
		this.keysToGet = new String[] {"item_id", "name", "item_notes", "modification_date", "is_latest", "is_assgined"};
	}	

	@Override
	public NounMetadata execute() {
		organizeKeys();
		HashMap<String, Object> map = new HashMap<>();
		
		// Getting the current time
		Calendar calendar = Calendar.getInstance();
		Date currDate = calendar.getTime();
		
		// Passing in the item_id
		map.put(this.keyValue.get(this.keysToGet[0]), null);
		// passing in the item name
		map.put(this.keyValue.get(this.keysToGet[1]), null);
		// passing in the item notes
		map.put(this.keyValue.get(this.keysToGet[2]), null);
		// passing in the date created or Modified
		map.put(this.keyValue.get(this.keysToGet[3]), currDate);
		// passing in if the item is the latest version
		map.put(this.keyValue.get(this.keysToGet[4]), null);
		// passing in if the item is assigned
		map.put(this.keyValue.get(this.keysToGet[5]), null);
		
		// Returning the map we just made above with default values
		return new NounMetadata(map, PixelDataType.MAP);
	}
	
}
