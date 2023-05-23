package dk.netarkivet.onbtools.browsertrix;

import java.io.ByteArrayInputStream;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jwat.common.ANVLRecord;
import org.jwat.common.Base32;
import org.jwat.common.ContentType;
import org.jwat.common.Uri;
import org.jwat.warc.WarcConstants;
import org.jwat.warc.WarcDigest;
import org.jwat.warc.WarcHeader;
import org.jwat.warc.WarcRecord;
import org.jwat.warc.WarcWriter;

public abstract class WarcBaseWriter {
    protected static final Log log = LogFactory.getLog(WarcBaseWriter.class);
    
    protected Uri writeInfoRecord(WarcWriter warcWriter, String filename, String jobid) throws Exception {
        ANVLRecord infoPayload = new ANVLRecord();
        infoPayload.addLabelValue("software", "JWAT, ONB WarcWriter");
        infoPayload.addLabelValue("conformsTo", "http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf");
        infoPayload.addLabelValue("isPartOf", jobid);
        
        Uri recordId = new Uri("urn:uuid:" + UUID.randomUUID().toString());
        byte[] payloadAsBytes = infoPayload.getUTF8Bytes();
        byte[] digestBytes = Sha1Util.sha1Digest(payloadAsBytes);
        WarcDigest blockDigest = WarcDigest.createWarcDigest("SHA1", digestBytes, 
        		"Base32", Base32.encodeArray(digestBytes));
        WarcRecord record = WarcRecord.createRecord(warcWriter);
        WarcHeader header = record.header;
        header.warcTypeIdx = WarcConstants.RT_IDX_WARCINFO;
        header.addHeader(WarcConstants.FN_WARC_RECORD_ID, recordId, null);
        header.addHeader(WarcConstants.FN_WARC_DATE, new Date(), null);
        header.addHeader(WarcConstants.FN_WARC_FILENAME, filename);
        header.addHeader(WarcConstants.FN_CONTENT_TYPE,  ContentType.parseContentType(WarcConstants.CT_APP_WARC_FIELDS), null);
        header.addHeader(WarcConstants.FN_CONTENT_LENGTH, Long.valueOf(payloadAsBytes.length), null);
        header.addHeader(WarcConstants.FN_WARC_BLOCK_DIGEST, blockDigest, null);
        warcWriter.writeHeader(record);
        ByteArrayInputStream bin = new ByteArrayInputStream(payloadAsBytes);
        warcWriter.streamPayload(bin);
        warcWriter.closeRecord();
        
        return recordId;
    }
}
