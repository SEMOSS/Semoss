package prerna.reactor.expression;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpSumProduct extends OpBasic {

	public OpSumProduct() {
		this.operation="sumproduct";
		this.keysToGet = new String[]{ReactorKeysEnum.ARRAY.getKey()};
		
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
		
	    NounMetadata sumProduct = new NounMetadata(sum, PixelDataType.CONST_DECIMAL);
        return sumProduct;
	}

	@Override
	public String getReturnType() {
		// TODO Auto-generated method stub
		return "double";
	}
}
