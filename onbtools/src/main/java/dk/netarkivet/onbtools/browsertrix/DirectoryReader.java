package dk.netarkivet.onbtools.browsertrix;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class DirectoryReader {
    protected static final Log log = LogFactory.getLog(DirectoryReader.class);
	
	protected String basedir;
	protected ArrayList<File> fileindex = new ArrayList<File>();
    
    String wildcardFilter = null;
    String[] extensions = null;
    
	public DirectoryReader(String aBasedir) {
		basedir = aBasedir;
	}
	
	public void addWildcardFilter(String aWildcardFilter) {
		wildcardFilter = aWildcardFilter;
	}
	
	public void addExtensionFilter(String[] aExtensions) {
		extensions = aExtensions;
	}
	
    public void read() {
        try {
        	init();
        	
            File inFile = new File(basedir);

            Collection<File> filelist = null;
            
    		if (wildcardFilter != null) {
                WildcardFileFilter wcf = new WildcardFileFilter(wildcardFilter);
                filelist = (Collection<File>) FileUtils.listFiles(inFile, wcf, null);
    		}
    		else {
    			String[] exts = {};
    			if (extensions != null) {
    				exts = extensions;
    			}
                filelist = (Collection<File>) FileUtils.listFiles(inFile, exts, false);
    		}
            
            Iterator<File> iter = filelist.iterator();

            log.info("start checking " + filelist.size() + " files.");

            while (iter.hasNext()) {
                File f = iter.next();
                fileindex.add(f);
            }
            
            postprocessing();            
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    public ArrayList<File> getFileindex() {
		return fileindex;
	}
    
	protected abstract void init();
	protected abstract void postprocessing();
}
