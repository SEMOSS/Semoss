package prerna.reactor.model;

public class Record {

	 private String u_name;
	 private String sys_id;
	 private String u_description;
	 private String sys_updated_by;
	 private String sys_created_on;
	public String getU_name() {
		return u_name;
	}
	public void setU_name(String u_name) {
		this.u_name = u_name;
	}
	public String getSys_id() {
		return sys_id;
	}
	public void setSys_id(String sys_id) {
		this.sys_id = sys_id;
	}
	public String getU_description() {
		return u_description;
	}
	public void setU_description(String u_description) {
		this.u_description = u_description;
	}
	public String getSys_updated_by() {
		return sys_updated_by;
	}
	public void setSys_updated_by(String sys_updated_by) {
		this.sys_updated_by = sys_updated_by;
	}
	public String getSys_created_on() {
		return sys_created_on;
	}
	public void setSys_created_on(String sys_created_on) {
		this.sys_created_on = sys_created_on;
	}
}
