package dk.netarkivet.onbtools.browsertrix;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class WarcDirectoryReader extends DirectoryReader {
    protected static final Log log = LogFactory.getLog(WarcDirectoryReader.class);
	
    ArrayList<BrowsertrixJob> jobfilelist = new ArrayList<BrowsertrixJob>();
	
	String[] extensions = { "warc.gz" };
    
	public WarcDirectoryReader(String aBasedir) {
		super(aBasedir);
	}
	
	public ArrayList<BrowsertrixJob> getJobfilelist() {
		return jobfilelist;
	}
	
	private void buildjobfilelist() {
		String jobid = null;
		ArrayList<File> filelist = new ArrayList<File>();
		for (File f : fileindex) {
			String token = null;
			try {
				String name = f.getName();
				StringTokenizer st = new StringTokenizer(name, "-");
				st.nextToken();
				st.nextToken();
				
				token = st.nextToken();
				token = token.substring(0, token.indexOf('.'));
				}
			catch(Exception e) {
				log.warn("no Browsertrix naming");
			}
			
			if (token != null && !token.equals(jobid)) {
				if (jobid != null) {
					jobfilelist.add(new BrowsertrixJob(jobid, filelist));
				}
				jobid = token;
				filelist = new ArrayList<File>();
			}
			
			filelist.add(f);
		}
		
		jobfilelist.add(new BrowsertrixJob(jobid, filelist));
	}

	@Override
	protected void init() {
		addExtensionFilter(extensions);
	}

	@Override
	protected void postprocessing() {
		Collections.sort(fileindex, new DefaultFileComparator());
		buildjobfilelist();
	}
}
