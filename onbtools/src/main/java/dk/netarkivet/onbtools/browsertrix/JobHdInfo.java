package dk.netarkivet.onbtools.browsertrix;

public class JobHdInfo {
	public JobHdInfo(Long aJobid, Long aHdid) {
		jobid = aJobid;
		harvestdefinitionid = aHdid;
	}
	
	public Long getJobid() {
		return jobid;
	}
	public void setJobid(Long jobid) {
		this.jobid = jobid;
	}
	public Long getHarvestdefinitionid() {
		return harvestdefinitionid;
	}
	public void setHarvestdefinitionid(Long harvestdefinitionid) {
		this.harvestdefinitionid = harvestdefinitionid;
	}
	Long jobid;
	Long harvestdefinitionid;
}
