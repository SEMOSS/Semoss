package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpArrayLength extends OpBasic {

    public OpArrayLength() {
          this.operation="getlength";
          this.keysToGet = new String[]{ReactorKeysEnum.ARRAY.getKey()};
    }

    @Override
    protected NounMetadata evaluate(Object[] values) {
          NounMetadata noun = new NounMetadata(values.length, PixelDataType.CONST_INT);
          return noun;
    }

    @Override
    public String getReturnType() {
          return "int";
    }

}

