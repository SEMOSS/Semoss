package prerna.ui.components.specific.iatdd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class AOAOverallScoreMap extends HashMap<String, Object> {

	public static final String FULFILLMENT_SCORE = "fScore";
	public static final String AND_PACKAGE_ARRAY = "ANDPackage";
	public static final String OR_PACKAGE_ARRAY ="ORPackage";
	public static final String ROW_AND_ARRAY = "rAnd";

	public void compileArrays(String vendor, String requirement, String packages, Double fScore, String ANDPackage, String ORPackage, boolean rAnd) {
		// mapping a vendor to all the requirements to the products that fulfill that requirement and their Fulfillment Score, ANDPackage, ORPackage & ROW_AND
		Map<String, Map<String, Map<String, Object>>> reqHash;
		Map<String, Map<String, Object>> packageHash;
		Map<String, Object> infoHash;

		if (this.containsKey(vendor)) {
			reqHash = (Map<String, Map<String, Map<String, Object>>>) this.get(vendor);
			if (reqHash.containsKey(requirement)) {
				packageHash = reqHash.get(requirement);
				if (packageHash.containsKey(packages)) {
					infoHash = packageHash.get(packages);
					if (ANDPackage != ""){
						((ArrayList<String>) infoHash.get(AND_PACKAGE_ARRAY)).add(ANDPackage);
					}
					if (ORPackage != ""){
						((ArrayList<String>) infoHash.get(OR_PACKAGE_ARRAY)).add(ORPackage);
					}
					infoHash.put(FULFILLMENT_SCORE, fScore);
					infoHash.put(ROW_AND_ARRAY, rAnd);
				} else {
					infoHash = new HashMap<String, Object>();
					ArrayList<String> andPackageList = new ArrayList<String>();
					if (ANDPackage != ""){
						andPackageList.add(ANDPackage);
					}
					infoHash.put(AND_PACKAGE_ARRAY, andPackageList);
					ArrayList<String> orPackageList = new ArrayList<String>();
					if (ORPackage != ""){
						orPackageList.add(ORPackage);
					}
					infoHash.put(OR_PACKAGE_ARRAY, orPackageList);
					infoHash.put(FULFILLMENT_SCORE, fScore);
					infoHash.put(ROW_AND_ARRAY, rAnd);
					packageHash.put(packages, infoHash);
				}
			} else {
				infoHash = new HashMap<String, Object>();
				ArrayList<String> andPackageList = new ArrayList<String>();
				if (ANDPackage != ""){
					andPackageList.add(ANDPackage);
				}
				infoHash.put(AND_PACKAGE_ARRAY, andPackageList);
				ArrayList<String> orPackageList = new ArrayList<String>();
				if (ORPackage != ""){
					orPackageList.add(ORPackage);
				}
				infoHash.put(OR_PACKAGE_ARRAY, orPackageList);
				infoHash.put(FULFILLMENT_SCORE, fScore);
				infoHash.put(ROW_AND_ARRAY, rAnd);
				packageHash = new HashMap<String, Map<String, Object>>();
				packageHash.put(packages, infoHash);
				reqHash.put(requirement, packageHash);
			}
		} else {
			infoHash = new HashMap<String,Object>();
			ArrayList<String> andPackageList = new ArrayList<String>();
			andPackageList.add(ANDPackage);
			infoHash.put(AND_PACKAGE_ARRAY, andPackageList);
			ArrayList<String> orPackageList = new ArrayList<String>();
			orPackageList.add(ORPackage);
			infoHash.put(OR_PACKAGE_ARRAY, orPackageList);
			infoHash.put(FULFILLMENT_SCORE, fScore);
			infoHash.put(ROW_AND_ARRAY, rAnd);
			packageHash = new HashMap<String, Map<String, Object>>();
			packageHash.put(packages, infoHash);
			reqHash = new HashMap<String, Map<String, Map<String,Object>>>();
			reqHash.put(requirement, packageHash);
			this.put(vendor, reqHash);
		}
	}

	/**
	 * Order results from best fulfillment to worst fulfillment
	 * 
	 * @param vendor
	 * @param requirement
	 */
	public List<Map<String, Object>> orderFulfillmentScoresForVendorAndRequirement(String vendor, String requirement) {
		Map<String, Map<String, Object>> packageMap = (Map<String, Map<String, Object>>) ((HashMap<String, Object>) this.get(vendor)).get(requirement);
		List<Map<String, Object>> orderedResults = new ArrayList<Map<String, Object>>();
		boolean firstInstance = true;

		for (String packageName : packageMap.keySet()) {
			Map<String, Object> infoMap = packageMap.get(packageName);

			int index = 0;

			if (firstInstance){
				Map<String, Object> resultHash = new HashMap<String, Object>();
				resultHash.putAll(infoMap);
				resultHash.put("packageName", packageName);
				orderedResults.add(index, resultHash);
				firstInstance = false;
			}

			else {
				for (Map<String, Object> results : orderedResults) {
					if (((double) results.get(FULFILLMENT_SCORE)) > ((double) infoMap.get(FULFILLMENT_SCORE))) {
						index++;
					} else {
						break;
					}
				}

				Map<String, Object> resultHash = new HashMap<String, Object>();
				resultHash.putAll(infoMap);
				resultHash.put("packageName", packageName);
				orderedResults.add(index, resultHash);
			}
		}
		//System.out.println("ORDERED RESULTS"+ requirement+ "" + orderedResults);
		return orderedResults;

	}

	public Map<String, Set<String>> getVendorReqHash() {

		Map<String,Set<String>> vendorReqHash = new HashMap<String, Set<String>>();
		Set<String> vendorSet = this.keySet();
		for(String vendor : vendorSet) {
			Set<String> requirements = ((HashMap<String, Object>) this.get(vendor)).keySet();
			vendorReqHash.put(vendor, requirements);
		}

		return vendorReqHash;
	}



}