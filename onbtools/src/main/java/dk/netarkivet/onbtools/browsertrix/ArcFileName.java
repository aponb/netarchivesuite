package dk.netarkivet.onbtools.browsertrix;

import java.io.File;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ArcFileName {
    final public static String SEPERATOR = "-";
    final public static String METADATA_MAGICWORD = "metadata";
    final public static String METADATA_DUMMYFILEID = "1";
    final public static String WARC_EXTENSION = ".warc";
    final public static String ARC_EXTENSION = ".arc";
    final public static String GZ_EXTENSION = ".gz";
    final public static String ARCGZ_EXTENSION = ARC_EXTENSION + GZ_EXTENSION;
    final public static String WARCGZ_EXTENSION = WARC_EXTENSION + GZ_EXTENSION;
    
	protected static final Log log = LogFactory.getLog(ArcFileName.class);
    
    private long jobId = -1;
    private int fileId = -1;
    private long harvestId = -1;
    private boolean isMetaDataFile = false;
    private boolean isValid = false;
    private String name;
	private String nameWithoutExtension;
    private String timestamp;
    private Date timestampDate;
	private String customName;
    private String extension = null;
    
    private boolean isArc = false;
    private boolean isWarc = false;
    private boolean isGz = false;
	boolean invalidAccepted = false;
    
    public ArcFileName(File aFile, boolean aInvalidAccepted) {
    	invalidAccepted = aInvalidAccepted;
    	if (aFile == null) {
            log.error("File is null");
            return;
    	}
    	
        name = aFile.getName();
        validate();
    }
    
    public ArcFileName(File aFile) {
    	this(aFile, false);
    }

    public ArcFileName(String aName, boolean aInvalidAccepted) {
    	invalidAccepted = aInvalidAccepted;
        if (aName == null) {
            log.error("Name is null");
            return;
        }
    	
        name = aName;
        validate();
    }
    
    public ArcFileName(String aName) {
    	this(aName, false);
    }
    
    private void buildNames(long aJobId, long aHarvestId, Date aTimeStamp, int aFileId, String aCustomName, String aExtension) {
    	timestampDate = aTimeStamp;
		nameWithoutExtension = String.valueOf(aJobId) + SEPERATOR + 
         	   String.valueOf(aHarvestId) + SEPERATOR +
         	   Utils.get14DigitDate(aTimeStamp) + SEPERATOR +
         	   StringUtils.leftPad(String.valueOf(aFileId), 5, '0') + SEPERATOR +
         	   aCustomName; 

         name = nameWithoutExtension + aExtension; 
    }
    
    public ArcFileName(long aJobId, long aHarvestId, Date aTimeStamp, int aFileId, String aCustomName, String aExtension) {
    	try {
    		buildNames(aJobId, aHarvestId, aTimeStamp, aFileId, aCustomName, aExtension);    		
            validate();
    	}
    	catch(Exception e) {
            log.error("Name can not be generated");
            return;
    	}
    }
    
    public String getUnzippedName() {
    	if (isGz == false) {
    		return name;
    	}
    	
    	return name.substring(0, name.length() - GZ_EXTENSION.length());
    }

    public String getZippedName() {
    	if (isGz == true) {
    		return name;
    	}
    	
    	return name + GZ_EXTENSION;
    }
    
    public String generateMetaDataFilename(boolean aCompressed) {
    	if (!isValid) {
    		log.error("not possible!");
    		return null;
    	}
    	
    	if (isMetaDataFile) {
    		return name;
    	}
    	
    	String metaname = String.valueOf(jobId) + SEPERATOR + METADATA_MAGICWORD + SEPERATOR + METADATA_DUMMYFILEID;
    	
    	if (isArc) {
    		metaname += ARC_EXTENSION;
    	}
    	else {
			if (isWarc)
	    		metaname += WARC_EXTENSION;
	    	}
    	
    	if (aCompressed) {
    		metaname += GZ_EXTENSION;
    	}
    	
    	return metaname;
	}
    
    private void validate() {
        if (name.endsWith(ARC_EXTENSION)) {
        	isArc = true;
        	extension = ARC_EXTENSION;
        }
        else if (name.endsWith(ARCGZ_EXTENSION)) {
        	isArc = true;
        	isGz = true;
        	extension = ARCGZ_EXTENSION;
		}
        else if (name.endsWith(WARC_EXTENSION)) {
        	isWarc = true;
        	isGz = false;
        	extension = WARC_EXTENSION;
		}
        else if (name.endsWith(WARCGZ_EXTENSION)) {
        	isWarc = true;
        	isGz = true;
        	extension = WARCGZ_EXTENSION;
		}
        else {
        	log.error("Extension of >" +  name + "< not valid");
        	return;
        }
        
        nameWithoutExtension = name.substring(0, name.length() - extension.length());
        
        String token;
        
        StringTokenizer st = new StringTokenizer(name, SEPERATOR);
        if (st.countTokens() == 1) {
        	String str = "no '" + SEPERATOR + "' found. Format invalid.";
        	if (isInvalidAccepted()) {
                log.info(str + " But accepted.");
        	}
        	else {
                log.error(str);
        	}
            return;
        }
       
        String jobIdStr = st.nextToken();
        
        try {
        	jobId = Long.parseLong(jobIdStr);
        }
        catch(Exception e) {
        	String str = "can't get jobId: "+ e.getMessage(); 
        	if (isInvalidAccepted()) {
                log.info(str + ". But accepted.");
        	}
        	else {
                log.error(str);
        	}
        	
            return;
        }
        
        token = st.nextToken();
        
        if (METADATA_MAGICWORD.equals(token)) {
            if (!st.hasMoreTokens()) {
            	String str = "arcfile invalid";
            	if (isInvalidAccepted()) {
                    log.info(str + ". But accepted.");
            	}
            	else {
                    log.error(str);
            	}
            	
            	return;
            }
            isMetaDataFile = true;
        }
        else {
            try {
            	harvestId = Long.parseLong(token);
            }
            catch(Exception e) {
            	String str = "can't get harvestId: "+ e.getMessage();
            	if (isInvalidAccepted()) {
                    log.info(str + ". But accepted.");
            	}
            	else {
                    log.error(str);
            	}
            	
                return;
            }
            
            try {
                timestamp = st.nextToken();
                if (timestamp.length() == Utils.TIMESTAMP14_LEN) {
                    timestampDate = Utils.parse14DigitDate(timestamp);
                }
                else if (timestamp.length() == Utils.TIMESTAMP17_LEN) {
                    timestampDate = Utils.parse17DigitDate(timestamp);
                }
                else {
                    throw new Exception("timestamp invalid len (should be 14 or 17)");
                }
            }
            catch(Exception e) {
            	String str = "can't get timestamp: "+ e.getMessage();
            	if (isInvalidAccepted()) {
                    log.info(str + ". But accepted.");
            	}
            	else {
                    log.error(str);
            	}
            	
                return;
            }

            try {
                token = st.nextToken();
                fileId = Integer.parseInt(token);
            }
            catch(Exception e) {
            	String str = "can't get fileId: "+ e.getMessage();
            	if (isInvalidAccepted()) {
                    log.info(str + ". But accepted.");
            	}
            	else {
                    log.error(str);
            	}
            	
                return;
            }
            
            try {
            	customName = st.nextToken();
            	customName = customName.substring(0, customName.length() - extension.length());
            }
            catch(Exception e) {
            	String str = "can't get customName: "+ e.getMessage();
            	if (isInvalidAccepted()) {
                    log.info(str + ". But accepted.");
            	}
            	else {
                    log.error(str);
            	}
            	
                return;
            }
            
        }

        isValid = true;
    }

    public long getJobId() {
        return jobId;
    }

    public boolean isMetaDataFile() {
        return isMetaDataFile;
    }

    public boolean isArc() {
        return isArc;
    }

    public boolean isWarc() {
        return isWarc;
    }

    public boolean isGz() {
        return isGz;
    }
    
    public boolean isValid() {
        return isValid;
    }

    public String getName() {
        return name;
    }
    
	public String getNameWithoutExtension() {
		return nameWithoutExtension;
	}
	
	public long getHarvestId() {
		return harvestId;
	}

	public long getFileId() {
		return fileId;
	}

	public String getTimeStamp() {
		return timestamp;
	}

    public String getCustomName() {
		return customName;
	}

	public String getExtension() {
		return extension;
	}
	
	public String toString() {
		if (!isValid()) {
			String str = "not valid";
			if (isInvalidAccepted()) {
				str += ", but accepted.";
			}
			return str;
		}
		
		if (isMetaDataFile()) {
			return getJobId() + SEPERATOR + METADATA_MAGICWORD;
		}
		else {
			return getJobId() + SEPERATOR + getHarvestId() + SEPERATOR + getTimeStamp() + SEPERATOR + getFileId();
		}
	}
	
    public boolean isInvalidAccepted() {
		return invalidAccepted;
	}
    
    public void increaseFileId() {
    	if (fileId != -1 && isMetaDataFile == false) {
    		fileId++;
    		buildNames(jobId, harvestId, timestampDate, fileId, customName, extension);    		
            validate();
    	}
    }
}
