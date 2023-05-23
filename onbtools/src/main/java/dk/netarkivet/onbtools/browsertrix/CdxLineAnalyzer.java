package dk.netarkivet.onbtools.browsertrix;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CdxLineAnalyzer {
    protected static final Log log = LogFactory.getLog(CdxLineAnalyzer.class);

    String line = null;
    String domainname = null;
    String hostname = null;
    long jobid = -1;
    boolean valid = false;
    boolean dnsEntry = false;
    
    boolean cdx9 = false;
    boolean cdx11 = false;

	int domainstatus;
	
	public static final int DOMAINSTATUS_OK = 0;
	public static final int DOMAINSTATUS_INVALID = 1;
	
	public static final String EMPTY_FIELD = "-";

	boolean gzposMarkerDetected = false;
    
    final static int URLKEY = 0;
    final static int TIMESTAMP = 1;
    final static int URL = 2;
    final static int MIMETYPE = 3;
    final static int RESPONSECODE = 4;
    final static int HASH = 5;
    final static int REDIRECT = 6;
    
    // nur bei CDX11
    final static int ROBOTFLAGS = 7;
    final static int LENGTH = 8;
    
    final static int OFFSET_CDX9 = 7;
    final static int ARCFILENAME_CDX9 = 8;

    int offset_idx = OFFSET_CDX9;
    int arcfilename_idx = ARCFILENAME_CDX9;
    
    final static int OFFSET_CDX11 = 9;
    final static int ARCFILENAME_CDX11 = 10;

    final static int NUMBEROFFIELDS_CDX9  = 9;
    final static int NUMBEROFFIELDS_CDX11 = 11;

	int numberOfCdxFields = NUMBEROFFIELDS_CDX9;
    
    final static int MAX_NUMBEROFFIELDS = NUMBEROFFIELDS_CDX11;
    
    String[] fields = new String[MAX_NUMBEROFFIELDS];
    
	OnbCdxDataAnalyzer onbCdxDataAnalayzer = null;

	boolean allowInvalidFiles;
    
//	# CDX 9 Format
//  [URLKEY, TIMESTAMP, ORIGINAL, MIMETYPE, STATUSCODE,
//   DIGEST, REDIRECT, OFFSET, FILENAME]
    
//	# CDX 11 Format
//  [URLKEY, TIMESTAMP, ORIGINAL, MIMETYPE, STATUSCODE,
//   DIGEST, REDIRECT, ROBOTFLAGS, LENGTH, OFFSET, FILENAME]

// vgl /org/netpreserve/commons/cdx/cdxrecord/CdxLineFormat.java ONB_METADATA_LINE
//	# ONB CDXDATA Format = CDX 11 Format + ONB Metadata
//  [URLKEY, TIMESTAMP, ORIGINAL, MIMETYPE, STATUSCODE,
//   DIGEST, REDIRECT, ROBOTFLAGS, LENGTH, OFFSET, FILENAME
//    ,CONTENT_LENGTH, ONB_IPADDRESS, ONB_PUID, PAYLOAD_LENGTH]
//
    
    public CdxLineAnalyzer(String aLine) {
        this(aLine, false);
    }

    public CdxLineAnalyzer(String aLine, boolean aAllowInvalidFiles) {
        line = aLine;
        allowInvalidFiles = aAllowInvalidFiles;
    }
    
    public static String getHashOnly(String line) {
    	try {
        	String[] tokens = line.split(" ");
        	
        	return tokens[HASH];
    	}
    	catch(Exception e) {
    		log.error("getHashOnly() throws exception. should not happen.");
    		throw new RuntimeException();
    	}
    }

    public static String getUrlOnly(String line) {
    	try {
        	String[] tokens = line.split(" ");
        	
        	return tokens[URL];
    	}
    	catch(Exception e) {
    		log.error("getUrlOnly() throws exception. should not happen.");
    		throw new RuntimeException();
    	}
    }
    
    
    public String getCdxLine() {
    	String line = "";
    	
    	try {
    		for (int i=0; i < numberOfCdxFields; i++) {
    			line += fields[i] + " ";
    		}
    		
    		return line.trim();
    	}
    	catch(Exception e) {
    		log.error("getCdxLine() throws exception. should not happen.");
    		throw new RuntimeException();
    	}
    }

    public boolean analyze() {
        if (line == null) {
            return false;
        }

        int currentIndex = 0;
        int currentField = 0;
        int endIndex = -1;
        try {
            while(true) {
            	if (currentField == NUMBEROFFIELDS_CDX11) {
             		log.debug("more than 11 fields. Storing the 11 fields as cdx11 and the rest as onbcdxdata");
             		onbCdxDataAnalayzer = new OnbCdxDataAnalyzer(line.substring(currentIndex));

             		setCdx11Flags();
             		onbCdxDataAnalayzer.analyze();
             		if (!onbCdxDataAnalayzer.isValid()) {
             			log.error("onbcdxdata are not valid." + onbCdxDataAnalayzer.getLine());
             			return false;
             		}
             		
             		break;
            	}
            	
            	endIndex = line.indexOf(' ', currentIndex);

            	if (endIndex == -1) {
                	fields[currentField] = line.substring(currentIndex);
            	}
            	else {
                	fields[currentField] = line.substring(currentIndex, endIndex);
            	}
            	
            	if (fields[currentField].length() == 0) {
            		fields[currentField] = EMPTY_FIELD;
            	}

            	currentField++;

            	if (endIndex == -1) {
            		 if (currentField == NUMBEROFFIELDS_CDX9) {
            			 cdx9 = true;
            			 break;
            		 }
            		 else if (currentField == NUMBEROFFIELDS_CDX11) {
            			 setCdx11Flags();
            			 break;
            		 }
            		 else {
                 		log.error("incorrect number of fields. Is " + currentField + " - should be " + NUMBEROFFIELDS_CDX9 + " or " + NUMBEROFFIELDS_CDX11 + ". Correct CDX Format?");
                 		return false;
            		 }
            	}
            	
            	currentIndex = endIndex + 1;
            }
        } catch(Exception e) {
        	log.error("Unexpected error: " + e.getMessage());
        	return false;
        }

        try {
            analyzeUrl();
            analyzeArcfilename();
        }
        catch(Exception e) {
            log.error(e);
            return false;
        }

        valid = true;
        return true;
    }
    
    private void setCdx11Flags() {
		 offset_idx = OFFSET_CDX11;
		 arcfilename_idx = ARCFILENAME_CDX11;
		 cdx11 = true;
		 numberOfCdxFields = NUMBEROFFIELDS_CDX11;
    }

    final String GZPOS_MARKER = ".gz,";
    
	private void analyzeArcfilename() {
		// auf gz, ueberpruefen
		// z.b. 50309-57-20161118190252314-00000-20855~webcrawler11.onb.ac.at~7012.arc.gz,18716

		int idx = -1;
		
		if ((idx = fields[arcfilename_idx].indexOf(GZPOS_MARKER)) > 0) {
			gzposMarkerDetected = true;
			
			String arcfilename = fields[arcfilename_idx];
			
			fields[arcfilename_idx] = arcfilename.substring(0, idx + GZPOS_MARKER.length() - 1);
			fields[offset_idx] = arcfilename.substring(idx + GZPOS_MARKER.length());
		}
		
        ArcFileName arcFileName = new ArcFileName(getArcfilename(), allowInvalidFiles);
        jobid = arcFileName.getJobId();
	}
	
    public boolean isGzposMarkerDetected() {
		return gzposMarkerDetected;
	}

	private void analyzeUrl() {
        try {
        	if (fields[URL].startsWith("dns:")) {
        		dnsEntry = true;
        	}
        	
            PublicSuffixService service = new PublicSuffixService(fields[URL]);
        	
            domainname = service.getDomainname();
            hostname = service.getHostname();
            domainstatus = DOMAINSTATUS_OK;
        }
        catch(IllegalArgumentException e) {
      	  	log.debug(e.getMessage() + ": >" + fields[URL] + "<");
      	  	domainname = EMPTY_FIELD;
      	  	hostname = EMPTY_FIELD;
            domainstatus = DOMAINSTATUS_INVALID;
        }
        catch(StringIndexOutOfBoundsException e) {
        	log.debug(e.getMessage() + ": >" + fields[URL] + "<");
        	domainname = EMPTY_FIELD;
        	hostname = EMPTY_FIELD;
            domainstatus = DOMAINSTATUS_INVALID;
          }
	}

	public String getLine() {
		return line;
	}

	public String getUrlKey() {
		return fields[URLKEY];
	}
    public String getTimestamp() {
		return fields[TIMESTAMP];
	}
	public String getUrl() {
		return fields[URL];
	}
	public String getMimetype() {
		return fields[MIMETYPE];
	}
	public String getResponsecode() {
		return fields[RESPONSECODE];
	}
	public String getHash() {
		return fields[HASH];
	}
	public String getRedirect() {
		return fields[REDIRECT];
	}
	
	public String getRobotflags() {
		return fields[ROBOTFLAGS];
	}

	public String getLength() {
		return fields[LENGTH];
	}
	
	public String getOffset() {
		return fields[offset_idx];
	}
	public String getArcfilename() {
		return fields[arcfilename_idx];
	}

	public String getDomainname() {
		return domainname;
	}

	public String getHostname() {
		return hostname;
	}
	
	public long getJobid() {
		return jobid;
	}

	public boolean isValid() {
		return valid;
	}
    public int getDomainstatus() {
		return domainstatus;
	}
    
	public boolean isDnsEntry() {
		return dnsEntry;
	}
	
	public boolean isCdx9() {
		return cdx9;
	}

	public boolean isCdx11() {
		return cdx11;
	}

	public boolean hasOnbCdxData() {
		if (onbCdxDataAnalayzer != null && onbCdxDataAnalayzer.isValid()) {
			return true;
		}

		return false;
	}

    public OnbCdxDataAnalyzer getOnbCdxDataAnalayzer() {
		return onbCdxDataAnalayzer;
	}
}
