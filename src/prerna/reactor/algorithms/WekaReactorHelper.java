package prerna.reactor.algorithms;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import prerna.engine.api.IHeadersDataRow;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Discretize;
import weka.filters.unsupervised.attribute.StringToNominal;

public class WekaReactorHelper {

	private WekaReactorHelper() {
		
	}
	
	public static Instances genInstances(String[] names, boolean[] isNumeric, int numRows) {
		int numAttr = names.length;
		ArrayList<Attribute> attributeList = new ArrayList<Attribute>();
		int i = 0;
		for(; i < numAttr; i++ ) {
			//special case for predictor since it must be nominal
			Attribute attribute = null;
			if(isNumeric[i]) {
				attribute = new Attribute(names[i]);
			} else {
				attribute = new Attribute(names[i], (List<String>) null);
			}
			attributeList.add(attribute);
		}
		
		//create the Instances Object to contain all the instance information
		Instances data = new Instances("data", attributeList, numRows);
		return data;
	}
	
	public static Instances fillInstances(Instances data, Iterator<IHeadersDataRow> it, boolean[] isNumeric, Logger logger) {
		int counter = 0;
		while(it.hasNext()) {
			IHeadersDataRow dataRow = it.next();
			String[] headers = dataRow.getHeaders();
			Object[] values = dataRow.getValues();

			Instance dataEntry = new DenseInstance(values.length);
			dataEntry.setDataset(data);
			int i = 0;
			int numAttr = headers.length;
			for(; i < numAttr; i++ ) {
				Object val = values[i];
				if(val == null) {
					dataEntry.setValue(i, "?");
				} else {
					if(isNumeric[i]) {
						dataEntry.setValue(i, ((Number) values[i]).doubleValue());
					} else {
						dataEntry.setValue(i, values[i].toString());
					}
				}
			}
			data.add(dataEntry);
			
			// logging
			if(counter % 100 == 0) {
				Configurator.setLevel(logger.getName(), Level.INFO);
				logger.info("Finished converting row = " + counter + " into WEKA instances object");
				Configurator.setLevel(logger.getName(), Level.OFF);
			}
			counter++;
		}
		Configurator.setLevel(logger.getName(), Level.INFO);

		// most things require the string values to be nominal
		// so make it nominal
		String range = "";
		for(int i = 0; i < isNumeric.length; i++) {
			if(!isNumeric[i]) {
				if(range.isEmpty()) {
					range += (i+1);
				} else {
					range += ","+ (i+1);
				}
			}
		}
		// convert string types to nominal
		if(!range.isEmpty()) {
			StringToNominal convert = new StringToNominal();
			String[] setOptions = new String[2];
			setOptions[0] = "-R";
			setOptions[1] = range;
			try {
				convert.setOptions(setOptions);
				convert.setInputFormat(data);
				data = Filter.useFilter(data, convert);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return data;
	}
	
	public static Instances discretizeAllNumericField(Instances data) {
		Discretize convert = new Discretize();
		String[] binOptions = new String[2];
		binOptions[0] = "-B";
		binOptions[1] = "50";
		try {
			convert.setOptions(binOptions);
			convert.setInputFormat(data);
			data = Filter.useFilter(data, convert);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return data;
	}
	
	public static Instances discretizeNumericField(Instances data, String index) {
		Discretize convert = new Discretize();
		String[] setOptions = new String[4];
		setOptions[0] = "-R";
		setOptions[1] = index + "-" + index;
		setOptions[2] = "-B";
		setOptions[3] = "50";
		try {
			convert.setOptions(setOptions);
			convert.setInputFormat(data);
			data = Filter.useFilter(data, convert);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return data;
	}
}
