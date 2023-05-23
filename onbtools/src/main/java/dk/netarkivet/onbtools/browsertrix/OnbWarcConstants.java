package dk.netarkivet.onbtools.browsertrix;

public class OnbWarcConstants {
	
	public static String CONVENTION_SEEDS_FILE = "seeds.txt";
	public static String CONVENTION_BROWSERTRIXSTARTUP_FILE = "browsertrix_startupscript.sh";
	
	public static String BROWSERTRIX_POSTDEDUPLICATOR = "metadata://webarchiv.onb.ac.at/crawl/logs/crawl.log?onbbrowsertrixpostdeduplicator";
	public static String BROWSERTRIX_POSTDEDUPLICATOR_REPORT = "metadata://webarchiv.onb.ac.at/crawl/reports/onbbrowsertrixpostdeduplicator-report.txt";
	public static String BROWSERTRIX_DEDUPLICATEREDUCTIONJOBS = "metadata://webarchiv.onb.ac.at/crawl/setup/duplicatereductionjobs";
	public static String BROWSERTRIX_SEEDS = "metadata://webarchiv.onb.ac.at/crawl/setup/" + CONVENTION_SEEDS_FILE;
	public static String BROWSERTRIX_LOG = "metadata://webarchiv.onb.ac.at/crawl/logs/crawl.log";
	public static String BROWSERTRIX_LOG_YAML = "metadata://webarchiv.onb.ac.at/crawl/logs/crawl.yaml";
	public static String BROWSERTRIX_BROWSERPROFILE = "metadata://webarchiv.onb.ac.at/crawl/setup/browsertrix_browserprofile.tar.gz";
	public static String BROWSERTRIX_STARTUPSCRIPT = "metadata://webarchiv.onb.ac.at/crawl/setup/" + CONVENTION_BROWSERTRIXSTARTUP_FILE;
	public static String BROWSERTRIX_DEDUPLICATIONREDUCTIONJOBS = "metadata://webarchiv.onb.ac.at/crawl/setup/duplicatereductionjobs";
	public static String BROWSERTRIX_ARCHIVFILES_REPORT = "metadata://webarchiv.onb.ac.at/crawl/report/archivefiles-report.txt";
}
