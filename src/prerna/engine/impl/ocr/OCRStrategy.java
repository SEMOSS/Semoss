package prerna.engine.impl.ocr;

public interface OCRStrategy {
    String getStrategyString();

    public static OCRStrategy getEnum(String search, Class<? extends Enum<? extends OCRStrategy>>... enumClasses) {
        if (search == null || enumClasses == null) {
            return null;
        }
        for (Class<? extends Enum<? extends OCRStrategy>> enumClass : enumClasses) {
            if (enumClass != null && enumClass.isEnum()) {
                for (Enum<?> constant : enumClass.getEnumConstants()) {
                    OCRStrategy strategy = (OCRStrategy) constant;
                    if (search.equals(strategy.getStrategyString())) {
                        return strategy;
                    }
                }
            }
        }
        return null;
    }
}
