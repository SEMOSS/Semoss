package prerna.sablecc2.reactor.app.upload;

public class FormUtility {

	public static String getTextComponent(String label) {
		return new StringBuilder("<div smss-form-model=\"" + label + "\">" + "{{item.model}}</div>\\n").toString();
	}

	public static String getInputComponent() {
		return new StringBuilder(" <smss-input ng-model=\"item.selected\">" + "</smss-input>\\n").toString();
	}

	public static String getDropdownComponent(String dataModelComp) {
		return new StringBuilder(
				"<smss-dropdown model=\"form.dataModel." + dataModelComp + ".selected\" options=\"form.dataModel."
						+ dataModelComp + ".options\"" + " placeholder=\"Select One\">\\n</smss-dropdown>\\n")
								.toString();
	}

	public static String getNumberPickerComponent() {
		return new StringBuilder("<smss-input type=\"number\" model=\"item.selected\">" + "</smss-input>\\n")
				.toString();
	}

	public static String getSliderComponent(String min, String max) {
		return new StringBuilder("<smss-slider min=\"" + min + "\" max=\"" + max
				+ "\" model=\"item.selected\" numerical=\"\"></smss-slider>\\n").toString();
	}

	public static String getTextAreaComponent() {
		return new StringBuilder("<smss-textarea ng-model=\"item.selected\"></smss-textarea>\\n").toString();
	}

	public static String getDatePickerComponent() {
		return new StringBuilder(
				"<smss-date-picker model=\"item.selected\" format=\"YYYY-MM-DD\"></smss-date-picker>\\n").toString();
	}

	public static String getSubmitComponent() {
		return new StringBuilder(
				"<div smss-form-model=\"Submit\" class=\"form-builder__submit\"><smss-btn class=\"smss-btn--primary\""
						+ " ng-click=\"form.runPixel(form.pixelModel.Insert.pixel)\">{{item.model}}</smss-btn></div>\\n")
								.toString();
	}

	public static String getDescriptionComponent(String description) {
		return new StringBuilder("<div class=\"smss-form-group__description\">" + description + "</div>\\n").toString();
	}
}
