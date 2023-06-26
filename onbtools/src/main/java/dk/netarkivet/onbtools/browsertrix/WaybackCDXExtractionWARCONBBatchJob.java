package dk.netarkivet.onbtools.browsertrix;

import dk.netarkivet.common.Constants;
import dk.netarkivet.common.exceptions.IOFailure;
import dk.netarkivet.wayback.batch.UrlCanonicalizerFactory;
import dk.netarkivet.wayback.batch.WaybackCDXExtractionWARCBatchJob;
import org.archive.io.warc.WARCRecord;
import org.archive.wayback.UrlCanonicalizer;
import org.archive.wayback.core.CaptureSearchResult;
import org.archive.wayback.resourceindex.cdx.SearchResultToCDXLineAdapter;
import org.archive.wayback.resourcestore.indexer.WARCRecordToSearchResultAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

public class WaybackCDXExtractionWARCONBBatchJob extends WaybackCDXExtractionWARCBatchJob {
    /** Logger for this class. */
    private static final Logger log = LoggerFactory.getLogger(WaybackCDXExtractionWARCONBBatchJob.class);

    /** Utility for converting an WArcRecord to a CaptureSearchResult (wayback's representation of a CDX record). */
    private WARCRecordToSearchResultAdapter aToSAdapter;

    /** Utility for converting a wayback CaptureSearchResult to a String representing a line in a CDX file. */
    private SearchResultToCDXLineAdapter srToCDXAdapter;

    /**
     * Constructor which set timeout to one day.
     */
    public WaybackCDXExtractionWARCONBBatchJob() {
        batchJobTimeout = Constants.ONE_DAY_IN_MILLIES;
    }

    @Override
    public void initialize(OutputStream os) {
        log.info("Starting a {}", this.getClass().getName());
        aToSAdapter = new WARCRecordToSearchResultAdapter();
        UrlCanonicalizer uc = UrlCanonicalizerFactory.getDefaultUrlCanonicalizer();
        aToSAdapter.setCanonicalizer(uc);
        srToCDXAdapter = new SearchResultToCDXLineAdapter();
    }
    /**
     * For each response WARCRecord it writes one CDX line (including newline) to the output. If an warcrecord cannot be
     * converted to a CDX record for any reason then any resulting exception is caught and logged.
     *
     * @param record the WARCRecord to be indexed.
     * @param os the OutputStream to which output is written.
     */
    @Override
    public void processRecord(WARCRecord record, OutputStream os) {
        CaptureSearchResult csr = null;
        try {
            csr = aToSAdapter.adapt(record);
        } catch (Exception e) {
            log.error("Exception processing WARC record:", e);
        }
        try {
            if (csr != null) {
                os.write(srToCDXAdapter.adapt(csr).getBytes());
                os.write(" ".getBytes());
                String urn = (String)record.getHeader().getHeaderValue("WARC-Record-ID");
                urn = urn.replaceAll("[<>]", "");
                os.write(urn.getBytes());
                os.write("\n".getBytes());
            }
        } catch (IOException e) {
            throw new IOFailure("Write error in batch job", e);
        } catch (Exception e) {
            log.error("Exception processing WARC record:", e);
        }
    }
}
