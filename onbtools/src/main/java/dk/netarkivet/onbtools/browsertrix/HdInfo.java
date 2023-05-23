package dk.netarkivet.onbtools.browsertrix;

public class HdInfo {
	public Long getHarvest_id() {
		return harvest_id;
	}
	public void setHarvest_id(Long harvest_id) {
		this.harvest_id = harvest_id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	Long harvest_id;
	String name;
}
