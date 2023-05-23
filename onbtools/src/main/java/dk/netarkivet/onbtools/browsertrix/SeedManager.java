package dk.netarkivet.onbtools.browsertrix;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SeedManager {
    protected static final Log log = LogFactory.getLog(SeedManager.class);

	ArrayList<String> seedList = new ArrayList<String>();
	ArrayList<String> domainList = new ArrayList<String>();
	File seedsfile;
	
	public SeedManager(File aSeedsfile) {
		seedsfile = aSeedsfile;
		readSeedsFromFile();
		buildDomainList();
	}
	
	private void readSeedsFromFile() {
		LineNumberReader f = null;
		
        try {
        	String line = "";
            f = new LineNumberReader(new FileReader(seedsfile));
            while ((line = f.readLine()) != null) {
        		if (line == null || line.length() == 0 || line.startsWith("#")) {
        			continue;
        		}
        		
        		seedList.add(line);
            }
            
         } catch (Exception e) {
        	 log.error("error reading " + seedsfile.getAbsolutePath());
         }
         finally {
             if (f != null) {
                 try {
                     f.close();
                 } catch (IOException e) { }
             }
         }
	}
	
	private void buildDomainList() {
		HashSet<String> domainset = new HashSet<String>();
		for (String seed: seedList) {
            PublicSuffixService service = new PublicSuffixService(seed);
            
            String domainname = null;
            try {
                domainname = service.getDomainname();
            }
            catch(IllegalArgumentException e) {
          	  continue;
            }

            if (!domainset.contains(domainname)) {
            	domainset.add(domainname);
            }
		}
		domainList = new ArrayList<>(domainset);		
	}
	
	public ArrayList<String> getSeedList() {
		return seedList;
	}

	public ArrayList<String> getDomainList() {
		return domainList;
	}
	
	public File getSeedsfile() {
		return seedsfile;
	}
}
