package dk.netarkivet.onbtools.browsertrix;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CdxLineReader {

	protected static final Log log = LogFactory.getLog(CdxLineReader.class);

	final static int MAXKEYSINCACHE = 1000000;
	String originalHash;
	String originalUrl;
	String originalLine;
	File originalCdxFile;
	static LRUMap cache = new LRUMap(MAXKEYSINCACHE);
	static boolean cacheRead = false;
	boolean caching = false;
	File cacheFile = null;
	boolean exceptionOccured = false;

	public CdxLineReader(String aOriginalHash, String aOriginalUrl, File aOriginalCdxFile) {
		this(aOriginalHash, aOriginalUrl, aOriginalCdxFile, false, null);
	}
	
	public CdxLineReader(String aOriginalHash, String aOriginalUrl, File aOriginalCdxFile, boolean aCaching, File aCacheFile) {
		originalHash = aOriginalHash;
		originalUrl = aOriginalUrl;
		originalCdxFile = aOriginalCdxFile;
		caching = aCaching;
		cacheFile = aCacheFile;
		
		if (cacheFile != null && cacheRead == false) {
			readFileCache(cacheFile);
			cacheRead = true;
		}
	}
	
	
	private void readFileCache(File aFile) {
		LineNumberReader f = null;
		
        try {
        	String line = "";
            f = new LineNumberReader(new FileReader(aFile));
            String key, value;
            int idx;
            int count = 0;
            while ((line = f.readLine()) != null) {
            	try {
                	idx = line.indexOf(" ");
                	key = line.substring(0, idx);
                	value = line.substring(idx + 1);
                	cache.put(key, value);
                	count++;
                	if (count >= MAXKEYSINCACHE) {
                		break;
                	}
            	}
            	catch(Exception e) {
            		log.warn("error filling cache. " + e.getMessage());
            	}
            }
         } catch (Exception e) {
        	 log.warn("error reading cache. " + e.getMessage());
        	 return;
         }
         finally {
             if (f != null) {
                 try {
                     f.close();
                 } catch (IOException e) { }
             }
         }
	}

	public String getOriginalLine() {
		return originalLine;
	}
	
	public boolean parsing() {
		String line;
		LineNumberReader reader = null;
		long lineNumber = 0;

		boolean hashFound = false;
		exceptionOccured = false;
		
		if (caching) {
			if ((line = (String)cache.get(originalHash)) != null) {
				if (CdxLineAnalyzer.getHashOnly(line).equals(originalHash)) {
					if (CdxLineAnalyzer.getUrlOnly(line).equals(originalUrl)) {
						originalLine = line;
						return true;
					}
				}
			}
		}
		
		try {
			reader = new LineNumberReader(new FileReader(originalCdxFile));

			while ((line = reader.readLine()) != null) {
				lineNumber++;
				if (CdxLineAnalyzer.getHashOnly(line).equals(originalHash)) {
					if (CdxLineAnalyzer.getUrlOnly(line).equals(originalUrl)) {
						hashFound = true;
						break;
					}
				}
			}

			reader.close();

			if (hashFound == false) {
				log.debug("duplicateHash not found");
				return false;
			}
			
			log.debug(originalHash + " found in Line " + lineNumber + " in " + originalCdxFile.getAbsolutePath());

			originalLine = line;

		} catch (Exception e) {
			exceptionOccured = true;
			log.error(e.getMessage());
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
		
		if (caching) {
			cache.put(originalHash, originalLine);
			if (cacheFile != null) {
				String cacheLine = originalHash + " " + originalLine;
				Utils.writeLineToFile(cacheLine, cacheFile.getAbsolutePath(), true);
			}
		}

		return hashFound;
	}
	
	public boolean hasExceptionOccurred() {
		return hasExceptionOccurred();
	}

}