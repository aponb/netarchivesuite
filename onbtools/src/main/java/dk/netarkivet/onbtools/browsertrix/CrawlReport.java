package dk.netarkivet.onbtools.browsertrix;

public class CrawlReport {
	SeedManager seedManager;
	DedupReport dedupReport = new DedupReport();
	
	
	public CrawlReport(SeedManager aSeedManager) {
		seedManager = aSeedManager;
	}
	
	public void add(String aUrl, String aContentType, long aPayload, boolean aDuplicate) {
		if (aDuplicate) {
			dedupReport.incrementDuplicate(aPayload);
		}
		else {
			dedupReport.increment(aPayload);
		}
	}
	
	public DedupReport getDedupReport() {
		return dedupReport;
	}
}
