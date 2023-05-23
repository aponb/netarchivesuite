package dk.netarkivet.onbtools.browsertrix;

import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CrawllogDirectoryReader extends DirectoryReader {
    protected static final Log log = LogFactory.getLog(CrawllogDirectoryReader.class);
	
	public CrawllogDirectoryReader(String aBasedir) {
		super(aBasedir);
	}

	@Override
	protected void init() {
	}

	@Override
	protected void postprocessing() {
		Collections.sort(fileindex, new DefaultFileComparator());
	}
}
