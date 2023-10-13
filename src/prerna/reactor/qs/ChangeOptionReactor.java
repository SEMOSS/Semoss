package prerna.reactor.qs;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.task.options.TaskOptions;

public class ChangeOptionReactor extends AbstractQueryStructReactor {

	// replaces part of the query - which ever the uses wishes to replace

	public ChangeOptionReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_PART.getKey() };
	}

	protected AbstractQueryStruct createQueryStruct() {

		// if the query struct is empty fill this into the insight
		prerna.query.querystruct.SelectQueryStruct sqs = null;
		List<Object> mapOptions = this.curRow.getValuesOfType(PixelDataType.MAP);
		// usually there should be just one
		// and that does it
		if (mapOptions != null && !mapOptions.isEmpty()) {
			for (int mapIndex = 0; mapIndex < mapOptions.size(); mapIndex++) {
				Map input = (Map) mapOptions.get(mapIndex);
				// see if the task options has an equivalent key
				Iterator keyIterator = input.keySet().iterator();

				while (keyIterator.hasNext()) {
					String key = (String) keyIterator.next();

					TaskOptions toptions = insight.getLastTaskOptions(key);
					sqs = insight.getLastQS(key);

					// this is needed because the collect block looks for the last key to get the
					// task options
					// forget thread safety - it is for birds
					insight.setLastPanelId(key);

					Map optionMap = toptions.getOptions();

					Map values = (Map) input.get(key);

					if (values != null && values.size() > 0 && optionMap != null && optionMap.containsKey(key))
						((Map) optionMap.get(key)).putAll(values);

				}
			}
		}
		// insight.setLastTaskOptions(toptions);

		if (sqs != null) {
			sqs.setLimit(-1);
		}

		return sqs;
	}
}
