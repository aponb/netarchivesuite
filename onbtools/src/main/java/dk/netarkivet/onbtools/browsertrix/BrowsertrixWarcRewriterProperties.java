package dk.netarkivet.onbtools.browsertrix;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BrowsertrixWarcRewriterProperties {
    protected static final Log log = LogFactory.getLog(BrowsertrixWarcRewriterProperties.class);
    
    // Browsertrix Directories
	String archiveDir;
	String crawlsDir;
    String logsDir;

    // directory for seeds.txt, browsertrix_startupscript.sh  
    String metadataDir;
    
    // directory where cdxfiles will be generated
    String cdxDir;
    
    // destination dir where all rewritten warcs will end up
    String destDir;
    
    // onb specifix cdx generator directories
    String dedupIndexDir;
    String dumpDir;

    // harvestdefinitionname all generated jobs belong to
    String harvestdefinitionname;
    
    // jmsbroker servername of used NAS-Environment 
    String jmsbroker;
    
    // port
    String jmsport;
    
    // some NAS specific environment information. From upload.sh
    String nasenvironmentname;
    String nasuploadapplicationname;
    String nasuploadapplicationinstanceid;

    // set true and now connection to NAS will be used. Dry run just to see how warc files will be generated. 1st job starts with id=1, and harvestdefinitionid=1 will be used
    boolean dryRun;

    // set true if you want to use deduplication
    boolean useDeduplication;

	public String getArchiveDir() {
		return archiveDir;
	}

	public void setArchiveDir(String archiveDir) {
		this.archiveDir = archiveDir;
	}

    public String getCrawlsDir() {
		return crawlsDir;
	}

	public void setCrawlsDir(String crawlsDir) {
		this.crawlsDir = crawlsDir;
	}

	public String getLogsDir() {
		return logsDir;
	}

	public void setLogsDir(String logsDir) {
		this.logsDir = logsDir;
	}
	
	public String getCdxDir() {
		return cdxDir;
	}

	public void setCdxDir(String cdxDir) {
		this.cdxDir = cdxDir;
	}

	public String getMetadataDir() {
		return metadataDir;
	}

	public void setMetadataDir(String metadataDir) {
		this.metadataDir = metadataDir;
	}

	public String getDestDir() {
		return destDir;
	}

	public void setDestDir(String destDir) {
		this.destDir = destDir;
	}

	public String getDedupIndexDir() {
		return dedupIndexDir;
	}

	public void setDedupIndexDir(String dedupIndexDir) {
		this.dedupIndexDir = dedupIndexDir;
	}

	public String getDumpDir() {
		return dumpDir;
	}

	public void setDumpDir(String dumpDir) {
		this.dumpDir = dumpDir;
	}

	public String getHarvestdefinitionname() {
		return harvestdefinitionname;
	}

	public void setHarvestdefinitionname(String harvestdefinitionname) {
		this.harvestdefinitionname = harvestdefinitionname;
	}

	public String getJmsbroker() {
		return jmsbroker;
	}

	public void setJmsbroker(String jmsbroker) {
		this.jmsbroker = jmsbroker;
	}

	public String getJmsport() {
		return jmsport;
	}

	public void setJmsport(String jmsport) {
		this.jmsport = jmsport;
	}

	public String getNasenvironmentname() {
		return nasenvironmentname;
	}

	public void setNasenvironmentname(String nasenvironmentname) {
		this.nasenvironmentname = nasenvironmentname;
	}

	public String getNasuploadapplicationname() {
		return nasuploadapplicationname;
	}

	public void setNasuploadapplicationname(String nasuploadapplicationname) {
		this.nasuploadapplicationname = nasuploadapplicationname;
	}

	public String getNasuploadapplicationinstanceid() {
		return nasuploadapplicationinstanceid;
	}

	public void setNasuploadapplicationinstanceid(String nasuploadapplicationinstanceid) {
		this.nasuploadapplicationinstanceid = nasuploadapplicationinstanceid;
	}
	
    public boolean isDryRun() {
		return dryRun;
	}

	public void setDryRun(boolean dryRun) {
		this.dryRun = dryRun;
	}

	public boolean isUseDeduplication() {
		return useDeduplication;
	}

	public void setUseDeduplication(boolean useDeduplication) {
		this.useDeduplication = useDeduplication;
	}
	
	public boolean validate() {
		log.info("dryRun:" + dryRun);
		log.info("useDeduplication:" + useDeduplication);
		
		log.info("archiveDir:" + archiveDir);
		if (archiveDir == null) {
			return false;
		}

		log.info("logsDir:" + logsDir);
		if (logsDir == null) {
			return false;
		}

		log.info("crawlsDir:" + crawlsDir);
		if (crawlsDir == null) {
			return false;
		}
		
		log.info("cdxDir:" + cdxDir);
		if (cdxDir == null) {
			return false;
		}

		log.info("metadataDir:" + metadataDir);
		if (metadataDir == null) {
			return false;
		}

		log.info("dumpDir:" + dumpDir);
		if (dumpDir == null) {
			return false;
		}

		log.info("harvestdefinitionname:" + harvestdefinitionname);
		if (harvestdefinitionname == null) {
			return false;
		}
		
		if (dryRun == false) {
			log.info("jmsbroker:" + jmsbroker);
			if (jmsbroker == null) {
				return false;
			}
	
			log.info("jmsport:" + jmsport);
			if (jmsport == null) {
				return false;
			}
	
			log.info("nasenvironmentname:" + nasenvironmentname);
			if (nasenvironmentname == null) {
				return false;
			}
	
			log.info("nasuploadapplicationname:" + nasuploadapplicationname);
			if (nasuploadapplicationname == null) {
				return false;
			}
	
			log.info("nasuploadapplicationinstanceid:" + nasuploadapplicationinstanceid);
			if (nasuploadapplicationinstanceid == null) {
				return false;
			}
		}
		
		return true;
	}
}
