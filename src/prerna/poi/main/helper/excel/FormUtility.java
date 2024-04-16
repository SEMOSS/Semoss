package prerna.poi.main.helper.excel;

public final class FormUtility {

	public static String getTextComponent(String label) {
		return new StringBuilder("<div smss-form-model=\"" + label + "\" class=\"smss-text\">{{item.model}}</div>\\n").toString();
	}

	public static String getInputComponent(String variableToBind) {
		return new StringBuilder("<smss-input ng-model=\"form.dataModel." + variableToBind + 
				".selected\"></smss-input>\\n").toString();
	}
	
	public static String getTypeAheadComponent(String variableToBind) {
		return new StringBuilder("<smss-typeahead ng-model=\"form.dataModel." + variableToBind + 
				".selected\" options=\"\"></smss-typeahead>\\n").toString();
	}

	public static String getDropdownComponent(String variableToBind) {
		return new StringBuilder("<smss-dropdown model=\"form.dataModel." + variableToBind + ".selected\" options=\"form.dataModel." + 
				variableToBind + ".options\" placeholder=\"Select One\">\\n</smss-dropdown>\\n").toString();
	}

	public static String getNumberPickerComponent(String variableToBind) {
		return new StringBuilder("<smss-input type=\"number\" ng-model=\"form.dataModel." + variableToBind + 
				".selected\"></smss-input>\\n").toString();
	}

	public static String getSliderComponent(String min, String max, String sensitivity) {
		return new StringBuilder("<smss-slider min=\"" + min + "\" max=\"" + max
				+ "\" sensitivity=\"" + sensitivity + "\" model=\"item.selected\" numerical=\"\"></smss-slider>\\n").toString();
	}

	public static String getSliderComponent(String min, String max, String sensitivity, String variableToBind) {
		return new StringBuilder("<smss-slider min=\"" + min + "\" max=\"" + max
				+ "\" sensitivity=\"" + sensitivity + "\" model=\"form.dataModel." + variableToBind + 
				".selected\"> numerical=\"\"></smss-slider>\\n").toString();
	}

	public static String getTextAreaComponent(String variableToBind) {
		return new StringBuilder("<smss-textarea ng-model=\"form.dataModel." + variableToBind + 
				".selected\"></smss-textarea>\\n").toString();
	}

	public static String getDatePickerComponent(String variableToBind) {
		return new StringBuilder("<smss-date-picker model=\"form.dataModel." + variableToBind + 
				".selected\"> format=\"YYYY-MM-DD\"></smss-date-picker>\\n").toString();
	}

	public static String getSubmitComponent(String crudOperation) {
		return new StringBuilder("<div smss-form-model=\"Submit\" class=\"form-builder__submit\"><smss-btn class=\"smss-btn--primary\"" + 
				" ng-click=\"form.runPixel(form.pixelModel." + crudOperation + ".pixel)\">{{item.model}}</smss-btn></div>\\n").toString();
	}

	public static String getDescriptionComponent(String description) {
		return new StringBuilder("<div class=\"smss-form-group__description\">" + description + "</div>\\n").toString();
	}
}
