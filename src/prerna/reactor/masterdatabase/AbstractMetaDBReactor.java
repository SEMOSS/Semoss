package prerna.reactor.masterdatabase;

import java.util.Vector;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Inputs for adding, updating and getting concept metadata
 *
 */
public abstract class AbstractMetaDBReactor extends AbstractReactor {
	
	protected static final String DESCRIPTION = "description";
	protected static final String VALUE_DELIMITER = ":::";
	protected static final String VALUES = "values";

	/**
	 * Get engine name input from pixel
	 * 
	 * @return
	 */
	public String getEngineId() {
		GenRowStruct engineGRS = this.store.getNoun(ReactorKeysEnum.DATABASE.getKey());
		if (engineGRS != null) {
			NounMetadata noun = engineGRS.getNoun(0);
			if (noun != null) {
				return noun.getValue() + "";
			}
		}
		throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.DATABASE.getKey() + " to be updated");
	}

	/**
	 * Get concept input from pixel
	 * 
	 * @return
	 */
	public String getConcept() {
		GenRowStruct conceptGRS = this.store.getNoun(ReactorKeysEnum.CONCEPT.getKey());
		if (conceptGRS != null) {
			NounMetadata noun = conceptGRS.getNoun(0);
			if (noun != null) {
				return noun.getValue() + "";
			}
		}
		throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.CONCEPT.getKey() + " to be updated");
	}

	/**
	 * Get description input from pixel
	 * 
	 * @return
	 */
	public String getDescription() {
		GenRowStruct descriptionGRS = this.store.getNoun(DESCRIPTION);
		if (descriptionGRS != null) {
			NounMetadata noun = descriptionGRS.getNoun(0);
			if (noun != null) {
				return noun.getValue() + "";
			}
		}
		throw new IllegalArgumentException("Need to define the " + DESCRIPTION + " to be updated");
	}

	/**
	 * Get values array as string from pixel input
	 * 
	 * @return
	 */
	public Vector<String> getValues() {
		GenRowStruct tagGRS = this.store.getNoun(VALUES);
		Vector<String> values = new Vector<String>();
		if (tagGRS != null) {
			for (int i = 0; i < tagGRS.size(); i++) {
				NounMetadata noun = tagGRS.getNoun(i);
				if (noun != null) {
					String value = noun.getValue() + "";
					values.add(value);
				}
			}
			return values;
		}
		throw new IllegalArgumentException("Need to define the " + VALUES + " to be updated");
	}
	
	///////////////////////// KEYS /////////////////////////////////////
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(DESCRIPTION)) {
			return "The description for the concept";
		} else if (key.equals(VALUES)) {
			return "The value to be updated for the concept";
		}
		else {
			return super.getDescriptionForKey(key);
		}
	}
	
}
