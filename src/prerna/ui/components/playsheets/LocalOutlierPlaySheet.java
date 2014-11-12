package prerna.ui.components.playsheets;

import java.util.ArrayList;

import prerna.algorithm.cluster.LocalOutlierFactorAlgorithm;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;

public class LocalOutlierPlaySheet extends GridPlaySheet{
	
	private int k;
	
	@Override
	public void createData() {
		super.createData();
		LocalOutlierFactorAlgorithm alg = new LocalOutlierFactorAlgorithm(list, names);
		alg.setK(k);
		alg.execute();
		
		list = alg.getMasterTable();
		names = alg.getNames();
		
		double[] lrd = alg.getLRD();
		double[] lof = alg.getLOF();
//		double[] zScore = alg.getZScore();
		double[] lop = alg.getLOP();
		
		ArrayList<Object[]> newList = new ArrayList<Object[]>();
		int numRows = list.size();
		int numCols = names.length;
		int newNumCols = numCols + 3;
		int i;
		int j;
		for(i = 0; i < numRows; i++) {
			Object[] newRow = new Object[newNumCols];
			Object[] row = list.get(i);
			for(j = 0; j <= numCols; j++) {
				if(j == numCols) {
//					newRow[j] = lrd[i];
					newRow[j] = Math.round(lrd[i] * 100) / 100.0;
					if(Double.isInfinite(lof[i])){
						newRow[j+1] = "Inf";
					} else {
//						newRow[j+1] = lof[i];
						newRow[j+1] = Math.round(lof[i] * 100) / 100.0;
					}
					if(Double.isNaN(lop[i])) {
						newRow[j+2] = "NaN";
					} else {
//						newRow[j+2] = zScore[i];
//						newRow[j+2] = Math.round(zScore[i] * 100) / 100.0;
						newRow[j+2] = String.format("%.0f%%",lop[i]*100);
					}
				} else {
					newRow[j] = row[j];
				}
			}
			newList.add(newRow);
		}
		list = newList;
		
		String[] newNames = new String[newNumCols];
		for(i = 0; i <= numCols; i++) {
			if(i == numCols) {
				newNames[i] = "LRD";
				newNames[i+1] = "LOF";
				newNames[i+2] = "LOP";
			} else {
				newNames[i] = names[i];
			}
		}
		names = newNames;
	}
	
	
	@Override
	public void setQuery(String query) {
		String[] querySplit = query.split("\\+\\+\\+");
		this.query = querySplit[0];
		this.k = Integer.parseInt(querySplit[1].trim());
	}
	
	@Override
	public Object getVariable(String varName, SesameJenaSelectStatement sjss){
		return sjss.getVar(varName);
	}
	
}
