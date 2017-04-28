package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class OpSumProduct extends OpBasic {

	public OpSumProduct() {
		this.operation="sumproduct";
	}

	@Override
	protected NounMetadata evaluate(Object[] values) {
		double sum=0;
		int length = ((Object[])values[0]).length;
		for (int i=0 ;i < length ; i++ ){
			double prod=1;
			for (Object objArr : values){
				double val = ((Number)((Object[])objArr)[i]).doubleValue();
				prod *= val;				
			}
			sum += prod;				
		}
		
	    NounMetadata sumProduct = new NounMetadata(sum, PkslDataTypes.CONST_DECIMAL);
        return sumProduct;
	}
}
