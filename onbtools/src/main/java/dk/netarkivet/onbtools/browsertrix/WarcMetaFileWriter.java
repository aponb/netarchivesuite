package dk.netarkivet.onbtools.browsertrix;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jwat.common.Uri;
import org.jwat.warc.WarcConstants;
import org.jwat.warc.WarcRecord;
import org.jwat.warc.WarcWriter;

public class WarcMetaFileWriter extends WarcBaseWriter {
    protected static final Log log = LogFactory.getLog(WarcMetaFileWriter.class);
    /** The URI for the warc info.*/
    
    WarcWriter warcWriter;
    String filename;
    long jobId;
    
    ArrayList<WarcResourceRecordParameter> warcResourceRecords = new ArrayList<WarcResourceRecordParameter>();
    
    public WarcMetaFileWriter(WarcWriter aWarcWriter, String aFilename, long aJobId) {
    	warcWriter = aWarcWriter;
    	filename = aFilename;
    	jobId = aJobId;
    }
    
    public void add(String aUri, String aContentType, File aPayloadfile) {
    	warcResourceRecords.add(new WarcResourceRecordParameter(aUri, aContentType, aPayloadfile));
    }
    
    public boolean write() {
        InputStream in = null;
    	
    	try {
    		Uri currentWarcInfoUUID = writeInfoRecord(warcWriter, filename, String.valueOf(jobId));
    		
    		for (WarcResourceRecordParameter parameters : warcResourceRecords) {
                WarcRecord warcRecord = WarcRecord.createRecord(warcWriter);
            	
                Uri recordId;
                recordId = new Uri("urn:uuid:" + UUID.randomUUID().toString());
                
                warcRecord.header.warcTypeIdx = WarcConstants.RT_IDX_RESOURCE;
                warcRecord.header.addHeader(WarcConstants.FN_WARC_RECORD_ID, recordId, null);
                warcRecord.header.addHeader(WarcConstants.FN_WARC_DATE, new Date(), null);
                warcRecord.header.addHeader(WarcConstants.FN_WARC_WARCINFO_ID, currentWarcInfoUUID, null);
                warcRecord.header.addHeader(WarcConstants.FN_CONTENT_TYPE, parameters.getContentType());
                warcRecord.header.addHeader(WarcConstants.FN_WARC_TARGET_URI, parameters.getUri());
                warcRecord.header.addHeader(WarcConstants.FN_WARC_TYPE,  WarcConstants.RT_RESOURCE);
                warcRecord.header.addHeader(WarcConstants.FN_CONTENT_LENGTH, new Long(parameters.getPayloadfile().length()), null);
                
                warcWriter.writeHeader(warcRecord);
                in = new FileInputStream(parameters.getPayloadfile());
                warcWriter.streamPayload(in);
                warcWriter.closeRecord();
    		}
    	}
    	catch(Exception e) {
    		log.error(e.getMessage());
    		return false;
    	}
    	finally {
            IOUtils.closeQuietly(in);
    	}
    	
    	return true;
    }
    
}
