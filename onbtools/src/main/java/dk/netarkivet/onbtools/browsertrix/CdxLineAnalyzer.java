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
    final static int OFFSET = 7;
    final static int ARCFILENAME = 8;

    final static int NUMBEROFFIELDS_CDX9  = 9;

	int numberOfCdxFields = NUMBEROFFIELDS_CDX9;
    
    final static int MAX_NUMBEROFFIELDS = NUMBEROFFIELDS_CDX9;
    
    String[] fields = new String[MAX_NUMBEROFFIELDS];
    
	OnbCdxDataAnalyzer onbCdxDataAnalayzer = null;

	boolean allowInvalidFiles;
    
//	# CDX 9 Format
//  [URLKEY, TIMESTAMP, ORIGINAL, MIMETYPE, STATUSCODE,
//   DIGEST, REDIRECT, OFFSET, FILENAME]
    
    public CdxLineAnalyzer(String aLine) {
        this(aLine, false);
    }

    public CdxLineAnalyzer(String aLine, boolean aAllowInvalidFiles) {
        line = aLine;
        allowInvalidFiles = aAllowInvalidFiles;
    }

	/* for quick response, requires valid cdx line (works for cdx9 and cdx11)
	 *
	 */

	public static String getFieldOnly(String line, int fieldid) {
		try {
			String[] tokens = line.split(" ");

			return tokens[fieldid];
		}
		catch(Exception e) {
			log.error("getFieldOnly() throws exception. should not happen. Called fieldid " + fieldid);
			throw new RuntimeException();
		}
	}

	public static String getHashOnly(String line) {
		return getFieldOnly(line, HASH);
	}

	public static String getUrlOnly(String line) {
		return getFieldOnly(line, URL);
	}

	public static String getResponsecodelOnly(String line) {
		return getFieldOnly(line, RESPONSECODE);
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
            	if (currentField == NUMBEROFFIELDS_CDX9) {
             		log.debug("more than 9 fields. Storing the 9 fields as cdx9 and the rest as onbcdxdata");
             		onbCdxDataAnalayzer = new OnbCdxDataAnalyzer(line.substring(currentIndex));

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
            			 break;
            		 }
            		 else {
                 		log.error("incorrect number of fields. Is " + currentField + " - should be " + NUMBEROFFIELDS_CDX9 + ". Correct CDX Format?");
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
    
    final String GZPOS_MARKER = ".gz,";
    
	private void analyzeArcfilename() {
		// auf gz, ueberpruefen
		// z.b. 50309-57-20161118190252314-00000-20855~webcrawler11.onb.ac.at~7012.arc.gz,18716

		int idx = -1;
		
		if ((idx = fields[ARCFILENAME].indexOf(GZPOS_MARKER)) > 0) {
			gzposMarkerDetected = true;
			
			String arcfilename = fields[ARCFILENAME];
			
			fields[ARCFILENAME] = arcfilename.substring(0, idx + GZPOS_MARKER.length() - 1);
			fields[OFFSET] = arcfilename.substring(idx + GZPOS_MARKER.length());
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
	
	public String getOffset() {
		return fields[OFFSET];
	}
	public String getArcfilename() {
		return fields[ARCFILENAME];
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
