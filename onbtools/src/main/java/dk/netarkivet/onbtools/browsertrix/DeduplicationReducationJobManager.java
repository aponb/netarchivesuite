package dk.netarkivet.onbtools.browsertrix;

import java.util.ArrayList;

public class DeduplicationReducationJobManager {
	ArrayList<Long> deduplicationreductionjobs = new ArrayList<Long>();
	
	public DeduplicationReducationJobManager() {
		
	}
	
	public void addJob(Long aJobId) {
		deduplicationreductionjobs.add(aJobId);
	}
	
	public String getMetadataInfoLine() {
		String line = "";
		for (Long jobid: deduplicationreductionjobs) {
			line += jobid;
			line += ",";
		}
		
		if (line.endsWith(",")) {
			return line.substring(0, line.length() - 1);
		}
		
		return line;
	}
}
