package reactors;

import java.util.ArrayList;
import java.util.List;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import util.HelperMethods;

public class ExampleReactor extends AbstractProjectReactor {

    // Note: Has access to protected variables defined in AbstractProjectReactor
    private static final String STRING = "string";

    public ExampleReactor() {
        this.keysToGet = new String[] {STRING, ReactorKeysEnum.ARRAY.getKey()};

        // 1 is required, 0 is optional
        this.keyRequired = new int[] {0, 0};
    }

    @Override
    protected NounMetadata doExecute() {
        
        // defaults to null if empty
        String inputString = this.keyValue.get(STRING);
        List<String> inputArray = getArray();

        // combine just to show everything loaded correct and we can see in output
        inputArray.add(inputString);

        // lets add the user id too since we can access User user from AbstractProjectReactor
        String userId = HelperMethods.getUserId(user);
        inputArray.add(userId);

        // Sample database helper method call - will not work as is
        // HelperMethods.queryDatabase(engine, null);

        return new NounMetadata(inputArray, PixelDataType.VECTOR);
    }

    public List<String> getArray() {
        List<String> inputStrings = new ArrayList<>();
        GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.ARRAY.getKey());
        if (grs != null && !grs.isEmpty()) {
            int size = grs.size();
            for (int i = 0; i < size; i++) inputStrings.add(grs.get(i).toString());
            return inputStrings;
        }
        int size = this.curRow.size();
        for (int i = 0; i < size; i++) inputStrings.add(this.curRow.get(i).toString());
        return inputStrings;
    }
}
