package dk.netarkivet.onbtools.browsertrix;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jwat.common.Payload;
import org.jwat.warc.WarcConstants;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import org.jwat.warc.WarcWriter;
import org.jwat.warc.WarcWriterFactory;

import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class WarcRewriter extends WarcBaseWriter {
    protected static final Log log = LogFactory.getLog(WarcRewriter.class);

	protected Gson gson = new GsonBuilder().create();	
    
    private static final String crawlDateFormatString = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final FastDateFormat crawlDateFormat = FastDateFormat.getInstance(crawlDateFormatString);
    
    WarcReader reader;
    WarcWriter writer;
    ArcFileName destWarc;

    BrowsertrixWarcRewriterProperties rewriterProperties;
    
    File outFile;
    File outMetaFile;
    
    File dedupLog;
    
    File cdxDir;
    File dumpDir;
    File dedupDir;
    File cacheFile;
    
    String destDir;
    String metadataDir;
    
    BrowsertrixJob browsertrixJob;
	ArrayList<FileInfo> regeneratedFileList = new ArrayList<FileInfo>();
    
    InputStream in = null;
    OutputStream out = null;
    
    Environment dbEnv;
    Database recordheaderList;
    Database revisitRecordList;
    Database idsToSkip;
    
    CrawlReport crawlReport;
    String deduplicatereductionjobsLine;
    
    long jobId;
    
    long MAXSIZE_WARC = 1000000000;
    long maxsizewarc = MAXSIZE_WARC;
    
    SeedManager seedManager;
    
    public void work() {
    	if (!init()) {
			return;
		}
    	
    	if (rewriterProperties.isUseDeduplication()) {
            buildHeaderList();
        	prepareDeduplicationRecords();
    	}
    	
    	rewrite();
    	writeMetadatafile();
    	
    	closedbs();
    }

    public WarcRewriter(long aJobId, long aHarvestId, BrowsertrixJob aBrowsertrixJob, BrowsertrixWarcRewriterProperties aRewriterProperties, DeduplicationReducationJobManager aDedupManager) {
    	rewriterProperties = aRewriterProperties;
    	browsertrixJob = aBrowsertrixJob;
    	
        jobId = aJobId;
        long harvestId = aHarvestId;
        
        int fileId = 0;
        String customName = "browsertrix";
        String extension = ".warc.gz";
        destDir = aRewriterProperties.getDestDir();
        metadataDir = aRewriterProperties.getMetadataDir();
        
        dedupDir = new File(aRewriterProperties.getDedupIndexDir());
        cdxDir = new File(aRewriterProperties.getCdxDir());
        
    	try {
			FileUtils.deleteDirectory(dedupDir);
			dedupDir.mkdirs();
		} catch (IOException e) {
			log.error(e.getMessage());
		}
        
    	Date firstDate = null;

    	try {
        	String[] tokens = browsertrixJob.getFilelist().get(0).getName().split("-");
    		firstDate = Utils.parse14DigitDate(tokens[1].substring(0, Utils.TIMESTAMP14_LEN));
    	}
    	catch(Exception e) {
    		log.warn("no Browsertrix warc files. Taking current Date for warc filenaming.");
    		firstDate = new Date();
    	}
    	
        destWarc = new ArcFileName(jobId, harvestId, firstDate, fileId, customName, extension);
        
        deduplicatereductionjobsLine = aDedupManager.getMetadataInfoLine();
        		
        outMetaFile = new File(destDir, destWarc.generateMetaDataFilename(true));
        dumpDir = new File(aRewriterProperties.getDumpDir());
        
        dedupLog = new File(dumpDir, "dedup.log");
        FileUtils.deleteQuietly(dedupLog);
        cacheFile = new File(dumpDir, "dedupcache");
    }
    
    public boolean init() {
		try {
			File seedsFile = new File(metadataDir, OnbWarcConstants.CONVENTION_SEEDS_FILE);
			seedManager = new SeedManager(seedsFile);

			crawlReport = new CrawlReport(seedManager);

			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			dbEnv = new Environment(dedupDir, envConfig);
			DatabaseConfig dbconf = new DatabaseConfig();
			dbconf.setAllowCreate(true);
			dbconf.setSortedDuplicates(false);//allow update
			recordheaderList = dbEnv.openDatabase(null, "recordheaderList", dbconf);
			revisitRecordList = dbEnv.openDatabase(null, "revisitRecordList", dbconf);
			idsToSkip = dbEnv.openDatabase(null, "idsToSkip", dbconf);
		}
		catch(Exception e) {
			log.error(e.getMessage());
			return false;
		}
		return true;
    }
    
    public File getOutFile() {
    	return outFile;
    }

    public File getOutMetaFile() {
    	return outMetaFile;
    }
    
    public void closedbs() {
		try {
			recordheaderList.close();
			revisitRecordList.close();
			idsToSkip.close();
			dbEnv.close();
		}
		catch(Exception e) {
			log.error(e.getMessage());
		}
        FileUtils.deleteQuietly(dedupLog);
    }
    
	private void buildHeaderList() {
        try {
			for (File f : browsertrixJob.getFilelist()) {
	    		log.info("builderHeaderList from " + f.getAbsolutePath());
	            in = new FileInputStream(f);

	            reader = WarcReaderFactory.getReader(in);
	            WarcRecord record;
	 
	            while ((record = reader.getNextRecord()) != null) {
	            	// only Response Records can be deduplicated
		    		if (record.header.warcTypeIdx != WarcConstants.RT_IDX_RESPONSE) {
		    			continue;
		    		}
	            	
	        		Payload pl = record.getPayload();
	        		
	        		long contentLength = pl.getTotalLength();
                	WarcRecordSer wrser = new WarcRecordSer(record, contentLength);
	        		String json = gson.toJson(wrser);
	        		
	                DatabaseEntry keyValue = new DatabaseEntry(record.header.warcRecordIdStr.getBytes("UTF-8"));
	                DatabaseEntry dataValue = new DatabaseEntry(json.getBytes("UTF-8"));
	                recordheaderList.put(null, keyValue, dataValue);        
	            }
	            
            	if (reader != null) {
                    reader.close();
            	}
            	
            	if (in != null) {
                    in.close();
            	}
			}
        }
        catch (Exception e) {
        	log.error(e.getMessage());
        }
        finally {
        	try {
            	if (reader != null) {
                    reader.close();
            	}
            	
            	if (in != null) {
                    in.close();
            	}
        	}
        	catch(Exception e2) {
        		// Should not happen
        		log.error(e2.getMessage());
        	}
        }
    }
    
    private void prepareDeduplicationRecords() {
		Cursor cursor = null;
		
		log.info("prepare duplicate records begin");
		try {
			cursor = recordheaderList.openCursor(null,  null);
			
		    DatabaseEntry foundKey = new DatabaseEntry();
		    DatabaseEntry foundData = new DatabaseEntry();
			
		    while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
	            byte[] key = foundKey.getData();
	            
	            WarcRecordSer wrs = gson.fromJson(new String(foundData.getData()), WarcRecordSer.class); 
	    		
	            if (!wrs.isDeduplicationCandidate()) {
	    			continue;
	    		}
	    		
	    		String cdxLine = findDuplicate(wrs.getWarcPayloadDigestStr(), wrs.getWarcTargetUriStr());
	    		// jetzt potentieller deduplication kandidat
	    		
	    		if (cdxLine != null) {
	    			// add to revisit List
	    			CdxLineAnalyzer cla = new CdxLineAnalyzer(cdxLine);
	    			if (cla.analyze()) {
	        			prepareRevisitRecord(wrs, cla);
	        			// als data payload len speichern
		        		//String json = gson.toJson(cla.getOnbCdxDataAnalayzer().getPayloadlength());
		        		String json = gson.toJson(wrs.getPayloadLen());
	        			idsToSkip.put(null, new DatabaseEntry(wrs.getWarcRecordIdStr().getBytes("UTF-8")), new DatabaseEntry(json.getBytes()));
	    			}
	    		}
	        }		    
	    } catch (Exception de) {
	        log.error(de.getMessage());
	    } finally {
	        if (cursor != null) {
				try {
					cursor.close();
				}
				catch(Exception e) {
					log.error(e.getMessage());
				}
	        }
	    }
		
		log.info("prepare duplicate records end");
	}
    
    // Content-Length in Warc-Record: Http-Header + Payload
    // Content-Length in http-Header: Payload
    private void prepareRevisitRecord(WarcRecordSer aWrs, CdxLineAnalyzer aCla) throws Exception {
    	WarcRevisitHelper wrh = new WarcRevisitHelper();
    	
    	wrh.setRefersToUrl(aCla.getUrl());
		wrh.setRefersToDateStr(Utils.convert14DigitDateToWarcDateStr(aCla.getTimestamp()));
    	
        // dedupLogLine erzeugen
        // nach https://heritrix.readthedocs.io/en/latest/operating.html#log-files
		// content-size: http-headerlen (from original) + payload len
		
        String annotations = "duplicate:\"" + aCla.getArcfilename() + "," + aCla.getOffset() + "," + aCla.getTimestamp() + "\",unwritten:identicalDigest,content-size:" + (aWrs.getHttpHeaderRaw().length + aWrs.getPayloadLen());
        
        String contentType = CdxLineAnalyzer.EMPTY_FIELD;
        
        if (aWrs.getHttpheader_contentType() != null) {
        	int endIdx = aWrs.getHttpheader_contentType().indexOf(" ");
        	if (endIdx == -1) {
            	contentType = aWrs.getHttpheader_contentType();
        	}
        	else {
            	contentType = aWrs.getHttpheader_contentType().substring(0, endIdx);
            	if (contentType.endsWith(";")) {
            		contentType = aWrs.getHttpheader_contentType().substring(0, contentType.length() - 1);
            	}
        	}
        }
        
        
        String dedupLogLine = 	aWrs.getWarcDateStr() + " " +								//  0	date
        						aWrs.getHttpheader_statusCodeStr() + " " + 					//  1	http status code
        						aWrs.getPayloadLen() + " " +								//  2	Document size: The size of the downloaded document in bytes. For HTTP, this is the size of content only. The size excludes the HTTP response headers. For DNS, the size field is the total size for the DNS response.
        						aWrs.getWarcTargetUriStr() + " " +							//  3	Target Uri
        						"-" + " " + 												//  4	Discovery Path dummy
        						"-" + " " + 												//  5	Referrer dummy
        						contentType + " " +											//  6 	content type
        						"-" + " " + 												//  7	Worker Thread ID dummy
        						"-" + " " + 												//  8	Fetch Timestamp dummy
        						aWrs.getWarcPayloadDigest() + " " + 						//	9	Digest
        						"-" + " " + 												// 10	Sourcetag dummy
        						annotations + " "											// 11   Annotations						
        						;

        wrh.setWrs(aWrs);
        wrh.setDedupLogLine(dedupLogLine);
        
        DatabaseEntry key = new DatabaseEntry(aWrs.getWarcRecordIdStr().getBytes());
        String json = gson.toJson(wrh);
        DatabaseEntry value = new DatabaseEntry(json.getBytes());
        revisitRecordList.put(null, key, value);
    }	

    
    private WarcRecord createRevisitRecord(WarcRevisitHelper aWrh) {
        WarcRecord warcRecord = WarcRecord.createRecord(writer);
        
        String httpHeaderStr = aWrh.getWrs().getHttpHeaderStr();
        
        // warc header
        warcRecord.header.warcTypeStr = "revisit";
        warcRecord.header.warcTargetUriStr = aWrh.getWrs().getWarcTargetUriStr();
        warcRecord.header.warcDateStr = aWrh.getWrs().getWarcDateStr();
        warcRecord.header.warcPayloadDigest = aWrh.getWrs().getWarcPayloadDigest();
        warcRecord.header.warcIpAddress = aWrh.getWrs().getWarcIpAddress();
        warcRecord.header.warcProfileStr = "http://netpreserve.org/warc/1.0/revisit/identical-payload-digest";
        warcRecord.header.warcRefersToTargetUriStr = aWrh.getRefersToUrl();
		warcRecord.header.warcRefersToDateStr = aWrh.getRefersToDateStr();
        warcRecord.header.warcRecordIdUri = aWrh.getWrs().getWarcRecordIdUri();
        warcRecord.header.contentType = aWrh.getWrs().getContentType();
        long httpHeaderLen = (long) httpHeaderStr.length();
        warcRecord.header.contentLength = httpHeaderLen;
        
        return warcRecord;
    }
    
    private void openNewWarcFileForWriting() throws Exception {
        outFile = new File(destDir, destWarc.getName());
        out = new FileOutputStream(outFile);
        writer = WarcWriterFactory.getWriter(out, true);
		writeInfoRecord(writer, outFile.getName(), String.valueOf(jobId));
    }
    
    public void rewrite() {
        in = null;
        out = null;
        
        log.info("rewrite begin");

        boolean changefile = true;
        long byteswritten = 0;
        
        regeneratedFileList = new ArrayList<FileInfo>();
        FileInfo currentFileInfo = null;

        try {
			for (File f : browsertrixJob.getFilelist()) {
        		log.info("rewrite " + f.getAbsolutePath());
				
	            in = new FileInputStream(f);
	            reader = WarcReaderFactory.getReader(in);
	            
	            WarcRecord record;

                while ((record = reader.getNextRecord()) != null) {
                	if (changefile) {
                		openNewWarcFileForWriting();
                		
                        currentFileInfo = new FileInfo(outFile);
                        regeneratedFileList.add(currentFileInfo);
                		byteswritten = 0;
                		changefile = false;
                	}

    	            currentFileInfo.incrementObjects();
                	
    	            if (rewriterProperties.isUseDeduplication()) {
                    	DatabaseEntry keyEntry = new DatabaseEntry(record.header.warcRecordIdStr.getBytes("UTF-8"));
                    	DatabaseEntry dataEntry = new DatabaseEntry();
                    	
                        if (idsToSkip.get(null, keyEntry, dataEntry, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            	            Long payloadlen = gson.fromJson(new String(dataEntry.getData()), Long.class);
            	            currentFileInfo.addBytes(payloadlen);
                            crawlReport.add(record.header.warcTargetUriStr, record.header.contentTypeStr, payloadlen, true);
                            continue;
                        }
    	            }

                	writer.writeHeader(record);
            		//log.info("write " + record.header.warcRecordIdStr + " for " + record.header.warcTargetUriStr);

        			if (record.hasPayload()) {
        				byte[] bytes = ByteStreams.toByteArray(record.getPayload().getInputStreamComplete());
        				writer.writePayload(bytes, 0, bytes.length);
        				byteswritten += bytes.length;
        	            currentFileInfo.addBytes(bytes.length);
                        crawlReport.add(record.header.warcTargetUriStr, record.header.contentTypeStr, bytes.length, false);
        			}
        			
        			if (byteswritten > maxsizewarc) {
                    	if (writer != null) {
                    		writer.close();
                    	}
                    	changefile = true;
                    	destWarc.increaseFileId();
        			}
                }
            }
		}
        catch(Exception e) {
    		log.error(e.getMessage());
        }
        finally {
        	if (in != null) {
                try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
        	}
        	if (writer != null) {
        		try {
					writer.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	}
        }
        
        try {
			FileUtils.touch(dedupLog);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        if (rewriterProperties.isUseDeduplication()) {
			Cursor cursor = null;
			changefile = true;
	    	destWarc.increaseFileId();
			
			try {
				cursor = revisitRecordList.openCursor(null,  null);
				
			    DatabaseEntry foundKey = new DatabaseEntry();
			    DatabaseEntry foundData = new DatabaseEntry();
				
			    while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
	            	if (changefile) {
	            		// if there are revisit records, write these in a final job file
	            		openNewWarcFileForWriting();
	            		
	                    currentFileInfo = new FileInfo(outFile);
	                    regeneratedFileList.add(currentFileInfo);
	            		byteswritten = 0;
	            		changefile = false;
	            	}
			    	
		            byte[] key = foundKey.getData();
		            WarcRevisitHelper wrh = gson.fromJson(new String(foundData.getData()), WarcRevisitHelper.class);
	
	            	writer.writeHeader(createRevisitRecord(wrh));
	
	            	if (wrh.getWrs().getHttpHeaderStr().length() > 0) {
	                    writer.writePayload(wrh.getWrs().getHttpHeaderStr().getBytes());
	            	}
					Utils.writeLineToFile(wrh.getDedupLogLine(), dedupLog.getAbsolutePath());
		            currentFileInfo.incrementObjects();
		        }		    
		    } catch (Exception de) {
		        log.error(de.getMessage());
		    } finally {
		        if (cursor != null) {
					try {
						cursor.close();
					}
					catch(Exception e) {
						log.error(e.getMessage());
					}
		        }
	        	if (writer != null) {
	        		try {
						writer.close();
					} catch (IOException e) {
						log.error(e.getMessage());
					}
	        	}
		    }
        }
        
        
        log.info("rewrite end");
    }

    public void writeMetadatafile() {
        out = null;
        
        log.info("write metadatafile begin");
        
        try {
            out = new FileOutputStream(outMetaFile);
            writer = WarcWriterFactory.getWriter(out, true);
        
            WarcMetaFileWriter wmw = new WarcMetaFileWriter(writer, outMetaFile.getName(), jobId);
            wmw.add(OnbWarcConstants.BROWSERTRIX_POSTDEDUPLICATOR, "text/plain", dedupLog);

            wmw.add(OnbWarcConstants.BROWSERTRIX_SEEDS, "text/plain", seedManager.getSeedsfile());

            File startup = new File(metadataDir, OnbWarcConstants.CONVENTION_BROWSERTRIXSTARTUP_FILE); 
            wmw.add(OnbWarcConstants.BROWSERTRIX_STARTUPSCRIPT, "text/x-shellscript", startup);

            if (rewriterProperties.isUseDeduplication()) {
                File dedup_report = new File(dumpDir, "onbbrowsertrixpostdeduplicator-report.txt"); 
                FileUtils.writeStringToFile(dedup_report, crawlReport.getDedupReport().report(), "UTF-8");
                wmw.add(OnbWarcConstants.BROWSERTRIX_POSTDEDUPLICATOR_REPORT, "text/plain", dedup_report);
            	
                File duplicatereductionjobs_file = new File(dumpDir, "duplicatereductionjobs.txt"); 
                Utils.writeLineToFile(deduplicatereductionjobsLine, duplicatereductionjobs_file.getAbsolutePath(), false);
                wmw.add(OnbWarcConstants.BROWSERTRIX_DEDUPLICATEREDUCTIONJOBS, "text/plain", duplicatereductionjobs_file);
            }

            if (browsertrixJob.getJobid() != null) {
                CrawllogDirectoryReader clr = null;
                
                File crawllog_file = new File(dumpDir, "crawl.log");
                // loop over all files in logs directory to find log files
                clr = new CrawllogDirectoryReader(rewriterProperties.getLogsDir());
                clr.addWildcardFilter("crawl-*-" + browsertrixJob.getJobid() + ".log");
                clr.read();
                crawllog_file.delete();
    	    	for (File f: clr.getFileindex()) {
    	    		Utils.appendFile(f, crawllog_file);
    	    	}
    	    	if (clr.getFileindex().size() > 0) {
    			    wmw.add(OnbWarcConstants.BROWSERTRIX_LOG, "text/plain", crawllog_file);
    	    	}
    		    
                File crawllog_yamlfile = new File(dumpDir, "crawl.yaml");
                // loop over all files in crawls directory to find log files
                clr = new CrawllogDirectoryReader(rewriterProperties.getCrawlsDir());
                clr.addWildcardFilter("crawl-*-" + browsertrixJob.getJobid() + ".yaml");
                clr.read();
                crawllog_yamlfile.delete();
    	    	for (File f: clr.getFileindex()) {
    	    		Utils.appendFile(f, crawllog_yamlfile);
    	    	}
    	    	
    	    	if (clr.getFileindex().size() > 0) {
    			    wmw.add(OnbWarcConstants.BROWSERTRIX_LOG_YAML, "text/plain", crawllog_yamlfile);
    	    	}
            }
            
            wmw.write();
            
            regeneratedFileList.add(new FileInfo(outMetaFile));
            log.info("write metadatafile end");
        }
        catch (Exception e) {
        	log.error(e.getMessage());
        }
        finally {
        	try {
            	if (writer != null) {
            		writer.close();
            	}
            	
            	if (out != null) {
                    out.close();
            	}
        	}
        	catch(Exception e2) {
        		// Should not happen
        		log.error(e2.getMessage());
        	}
        }
    }
    
	String[] cdxextensions = { "cdx" };
    
    private String findDuplicate(String aDigest, String aOriginalurl) {
    	if (aDigest == null || aOriginalurl == null) {
    		return null;
    	}
    	
        try {
            Collection<File> filelist = (Collection<File>) FileUtils.listFiles(cdxDir, cdxextensions, false);
            Iterator<File> iter = filelist.iterator();

            while (iter.hasNext()) {
                File f = iter.next();
                
            	aDigest = aDigest.replace("sha1:", "");
                CdxLineReader cdxLineReader;
            	
        		try {
        			cdxLineReader = new CdxLineReader(aDigest, aOriginalurl, f, true, cacheFile);
        		} catch (Exception e) {
        			log.error(e.getMessage());
        			return null;
        		}
                if (cdxLineReader.parsing()) {
                	return cdxLineReader.getOriginalLine();
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    	return null;
    }
    
    public ArrayList<FileInfo> getRegeneratedFileList() {
		return regeneratedFileList;
	}
}
