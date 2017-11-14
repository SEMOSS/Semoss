package prerna.sablecc2.reactor.masterdatabase;

import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public abstract class AbstractMetaDBReactor extends AbstractReactor {

	/**
	 * This reactor retrieves the inputs for adding, updating, deleting, and
	 * getting concept metadata
	 */

	private static final String ENGINE = "engine";
	private static final String CONCEPT = "concept";
	private static final String DESCRIPTION = "description";
	private static final String TAG = "tag";
	protected static final String TAG_DELIMITER = ":::";
	private static final String VALUES = "values";

	/**
	 * This method is used to get the engine where an update is required in
	 * local master
	 * 
	 */
	public String getEngine() {
		GenRowStruct engineGRS = this.store.getNoun(ENGINE);
		if (engineGRS != null) {
			NounMetadata noun = engineGRS.getNoun(0);
			if (noun != null) {
				return noun.getValue() + "";
			}
		}
		throw new IllegalArgumentException("Need to define the " + ENGINE + " to be updated");
	}

	/**
	 * This method is used to get the concept where an update is required in
	 * local master
	 * 
	 */
	public String getConcept() {
		GenRowStruct conceptGRS = this.store.getNoun(CONCEPT);
		if (conceptGRS != null) {
			NounMetadata noun = conceptGRS.getNoun(0);
			if (noun != null) {
				return noun.getValue() + "";
			}
		}
		throw new IllegalArgumentException("Need to define the " + CONCEPT + " to be updated");
	}

	/**
	 * This method is used to get the description where an update is required in
	 * local master
	 * 
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
	 * This method is used to get the description where an update is required in
	 * local master
	 * 
	 */
	public String getTag() {
		GenRowStruct tagGRS = this.store.getNoun(TAG);
		if (tagGRS != null) {
			NounMetadata noun = tagGRS.getNoun(0);
			if (noun != null) {
				return noun.getValue() + "";
			}
		}
		throw new IllegalArgumentException("Need to define the " + TAG + " to be updated");
	}

	
	public Vector<String> getNewValue() {
		GenRowStruct tagGRS = this.store.getNoun(VALUES);
		Vector<String> values = new Vector<String>();
		if (tagGRS != null) {
			for (int i = 0; i < tagGRS.size(); i++) {
				NounMetadata noun = tagGRS.getNoun(i);
				if (noun != null) {
					String value = noun.getValue() + "";
					if (value.length() > 0) {
						values.add(value);
					}
				}
			}
			if (values.size() > 0) {
				return values;
			}
		}
		throw new IllegalArgumentException("Need to define the " + VALUES + " to be updated");
	}

}
