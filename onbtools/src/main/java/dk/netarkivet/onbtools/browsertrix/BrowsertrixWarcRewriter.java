package dk.netarkivet.onbtools.browsertrix;

import java.io.*;
import java.util.ArrayList;

import dk.netarkivet.common.CommonSettings;
import dk.netarkivet.common.utils.batch.BatchLocalFiles;
import dk.netarkivet.common.utils.batch.FileBatchJob;
import dk.netarkivet.wayback.batch.WaybackCDXExtractionWARCBatchJob;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dk.netarkivet.common.distribute.HTTPRemoteFileRegistry;
import dk.netarkivet.common.distribute.JMSConnectionFactory;
import dk.netarkivet.common.distribute.arcrepository.ArcRepositoryClientFactory;
import dk.netarkivet.common.distribute.arcrepository.HarvesterArcRepositoryClient;
import dk.netarkivet.common.utils.Settings;

public class BrowsertrixWarcRewriter {
    protected static final Log log = LogFactory.getLog(BrowsertrixWarcRewriter.class);

	protected String propertyfilename;

	File seedsFile;
	ArrayList<String> seeds = new ArrayList<String>();
	DeduplicationReducationJobManager dedupJobManager = new DeduplicationReducationJobManager();

	BrowsertrixWarcRewriterProperties properties = new BrowsertrixWarcRewriterProperties();

	protected String nasDB_SPECIFICS_CLASS;
	protected String nasDB_USERNAME;
	protected String nasDB_PASSWORD;
	protected String nasDB_BASE_URL;
	protected String nasDB_MACHINE;
	protected String nasDB_PORT;
	protected String nasDB_DIR;

	protected boolean initialized = false;

	public boolean isInitialized() {
		return initialized;
	}
	private void readConfig() {
		try {
			PropertiesConfiguration config = Utils.getPropertiesConfiguration(propertyfilename);
			
			properties.setArchiveDir(config.getString("archiveDir"));
			properties.setCrawlsDir(config.getString("crawlsDir"));
			properties.setLogsDir(config.getString("logsDir"));
			properties.setCdxDir(config.getString("cdxDir"));
			properties.setMetadataDir(config.getString("metadataDir"));
			properties.setDestDir(config.getString("destDir"));
			properties.setDumpDir(config.getString("dumpDir"));
			properties.setDedupIndexDir(config.getString("dedupIndexDir"));
			properties.setHarvestdefinitionname(config.getString("harvestdefinitionname"));
			properties.setJmsbroker(config.getString("jmsbroker"));
			properties.setJmsport(config.getString("jmsport"));
			properties.setNasenvironmentname(config.getString("nasenvironmentname"));
			properties.setNasuploadapplicationname(config.getString("nasuploadapplicationname"));
			properties.setNasuploadapplicationinstanceid(config.getString("nasuploadapplicationinstanceid"));
			
			properties.setDryRun(config.getBoolean("dryRun", false));
			properties.setUseDeduplication(config.getBoolean("useDeduplication", true));

			if (!properties.isDryRun()) {
				readNasConfig();
			}

		} catch (ConfigurationException e) {
			log.error(e.getMessage());
			throw new RuntimeException("Init error: " + e.getMessage());
		}
	}

	protected void readNasConfig() {
		try {
			PropertiesConfiguration config = Utils.getPropertiesConfiguration(propertyfilename);

			nasDB_SPECIFICS_CLASS = config.getString("nasDB_SPECIFICS_CLASS");
			if (nasDB_SPECIFICS_CLASS == null) {
				throw new ConfigurationException("nasDB_SPECIFICS_CLASS is null");
			}
			log.info("nasDB_SPECIFICS_CLASS:" + nasDB_SPECIFICS_CLASS);

			nasDB_USERNAME = config.getString("nasDB_USERNAME");
			if (nasDB_USERNAME == null) {
				throw new ConfigurationException("nasDB_USERNAME is null");
			}
			log.info("nasDB_USERNAME:" + nasDB_USERNAME);

			nasDB_PASSWORD = config.getString("nasDB_PASSWORD");
			if (nasDB_PASSWORD == null) {
				throw new ConfigurationException("nasDB_PASSWORD is null");
			}
			log.info("nasDB_PASSWORD:" + nasDB_PASSWORD);

			nasDB_BASE_URL = config.getString("nasDB_BASE_URL");
			if (nasDB_BASE_URL == null) {
				throw new ConfigurationException("nasDB_BASE_URL is null");
			}
			log.info("nasDB_BASE_URL:" + nasDB_BASE_URL);

			nasDB_MACHINE = config.getString("nasDB_MACHINE");
			if (nasDB_MACHINE == null) {
				throw new ConfigurationException("nasDB_MACHINE is null");
			}
			log.info("nasDB_MACHINE:" + nasDB_MACHINE);

			nasDB_PORT = config.getString("nasDB_PORT");
			if (nasDB_PORT == null) {
				throw new ConfigurationException("nasDB_PORT is null");
			}
			log.info("nasDB_PORT:" + nasDB_PORT);

			nasDB_DIR = config.getString("nasDB_DIR");
			if (nasDB_DIR == null) {
				throw new ConfigurationException("nasDB_DIR is null");
			}
			log.info("nasDB_DIR:" + nasDB_DIR);

			Settings.set("settings.harvester.datamodel.domain.nextJobId", "0");
		} catch (ConfigurationException e) {
			throw new RuntimeException("config error: " + e.getMessage());
		}
	}

	protected void initNasSettings() {
		Settings.set(CommonSettings.DB_SPECIFICS_CLASS, nasDB_SPECIFICS_CLASS);
		Settings.set(CommonSettings.DB_USERNAME, nasDB_USERNAME);
		Settings.set(CommonSettings.DB_PASSWORD, nasDB_PASSWORD);
		Settings.set(CommonSettings.DB_BASE_URL, nasDB_BASE_URL);
		Settings.set(CommonSettings.DB_MACHINE, nasDB_MACHINE);
		Settings.set(CommonSettings.DB_PORT, nasDB_PORT);
		Settings.set(CommonSettings.DB_DIR, nasDB_DIR);
	}

	private ArrayList<String> readSeedsFromFile(File aSeedsfile) {
		ArrayList<String> seedlist = new ArrayList<String>();
		
		LineNumberReader f = null;
		
        try {
        	String line = "";
            f = new LineNumberReader(new FileReader(aSeedsfile));
            while ((line = f.readLine()) != null) {
        		if (line == null || line.length() == 0 || line.startsWith("#")) {
        			continue;
        		}
        		
        		seedlist.add(line);
            }
            
            return seedlist;
         } catch (Exception e) {
        	 log.error("error reading " + aSeedsfile.getAbsolutePath());
        	 return null;
         }
         finally {
             if (f != null) {
                 try {
                     f.close();
                 } catch (IOException e) { }
             }
         }
	}
	
	private JobHdInfo createJob() throws Exception {
		log.info("create job");

		NasSeedManagerProperties nasSeedManagerProps = new NasSeedManagerProperties();
		
		nasSeedManagerProps.setSeedlist(seeds);
		nasSeedManagerProps.setCreateconfiguration(true);
		nasSeedManagerProps.setEmptyHarvestdefinition(true);
		nasSeedManagerProps.setClearSeedlistBeforeAdding(true);
		nasSeedManagerProps.setConfigurationname("browsertrix");
		nasSeedManagerProps.setSeedlistname("browsertrix");
		nasSeedManagerProps.setAddToHarvestdefinition(true);
		nasSeedManagerProps.setHarvestdefinitionname(properties.getHarvestdefinitionname());
		nasSeedManagerProps.setOrderxmlname("default_orderxml");
		
		NasSeedManager seedManager = new NasSeedManager(nasSeedManagerProps);
		JobHdInfo jobid = seedManager.createFinishedJobForDomainsWithSeedList();
		
		log.info("job created with id " + jobid.getJobid());
		
		return jobid;
	}
	
	
	private boolean rewrite() {
		try {
			WarcDirectoryReader reader = new WarcDirectoryReader(properties.getArchiveDir());
			reader.read();
			ArrayList<BrowsertrixJob> joblist = reader.getJobfilelist();
			
			File cdxFile = null;
			
			long dryRunJobId = 1;
			for (BrowsertrixJob browsertrixJob : joblist) {
				JobHdInfo jobinfo;
				
	    		if (properties.isDryRun()) {
					jobinfo = new JobHdInfo(dryRunJobId, 1L);
					dryRunJobId++;
	    		}
	    		else {
					jobinfo = createJob();
	    		}
				
				WarcRewriter rw = new WarcRewriter(jobinfo.getJobid(), jobinfo.getHarvestdefinitionid(), browsertrixJob, properties, dedupJobManager);
				rw.work();
				
				cdxFile = new File(properties.getCdxDir(), jobinfo.getJobid() + ".cdx");

				FileInfo total = new FileInfo();

				File tmpCdx = new File(properties.getDumpDir(), "tmpcdx");
				FileUtils.deleteQuietly(cdxFile);
				
				for (FileInfo fi : rw.getRegeneratedFileList()) {
					BatchLocalFiles blafWarc = new BatchLocalFiles(new File[] { fi.getFile()});
					FileBatchJob job = new WaybackCDXExtractionWARCBatchJob();

			        OutputStream os = new ByteArrayOutputStream();
			        blafWarc.run(job, os);
			        os.flush();

			        FileOutputStream foStream = new FileOutputStream(tmpCdx);
			        ((ByteArrayOutputStream)os).writeTo(foStream);
			        foStream.close();

			        Utils.appendFile(tmpCdx, cdxFile);
					
		    		if (!properties.isDryRun()) {
			    		boolean status = uploadFile(fi.getFile());
			    		log.info("upload success:" + status);
		    		}
		    		total.addBytes(fi.getBytes());
		    		total.addObjects(fi.getObjects());
				}
				
				dedupJobManager.addJob(jobinfo.getJobid());
			}
		}
		catch(Exception e) {
			log.error(e.getMessage());
		}
		
		return true;
	}
	
	private boolean uploadFile(File aFile) {
        HarvesterArcRepositoryClient arcrep = null;
        try {
            log.info("Connecting to ArcRepository");
            arcrep = ArcRepositoryClientFactory.getHarvesterInstance();
            log.info("Uploading file " + aFile.getAbsolutePath() + " ...");
            
            arcrep.store(aFile);
            
            log.info("Uploaded.");
            
            return true;
        }
        catch(Exception e) {
        	log.error(e.getMessage());
        } finally {
            // Close connections
            if (arcrep != null) {
                arcrep.close();
            }
            JMSConnectionFactory.getInstance().cleanup();
        }
        
        return false;
	}
	
	private void updateJobdata() {
		//TODO update here Jobdata in NAS when necessary
		
	}

	public static void main(String[] args) {
		String propertyfilename =  "BrowsertrixWarcBaseRewriter.properties";
		
		if (args.length == 1) {
			propertyfilename = args[0];
		}
		
		BrowsertrixWarcRewriter rewriter = new BrowsertrixWarcRewriter(propertyfilename);
		rewriter.work();
	}
	
	public BrowsertrixWarcRewriter(String aPropertyfilename) {
		propertyfilename = aPropertyfilename;
	}

	protected boolean init() {
		log.info("init begin");

		try {
			if (!properties.validate()) {
				log.error("Property validation error");
				return false;
			}
			
			if (!properties.isDryRun()) {
				initNasSettings();
				
				/* nas jms configs */
				
		        Settings.set("settings.common.jms.class", "dk.netarkivet.common.distribute.JMSConnectionSunMQ");
		        Settings.set("settings.common.jms.broker", properties.getJmsbroker());
		        
		        Settings.set("settings.common.jms.port", properties.getJmsport());
		        Settings.set("settings.common.environmentName", properties.getNasenvironmentname());
		        Settings.set("settings.common.applicationName", properties.getNasuploadapplicationname());
		        Settings.set("settings.common.applicationInstanceId", properties.getNasuploadapplicationinstanceid());
			}
			
			FileUtils.deleteQuietly(new File(properties.getDumpDir()));
			new File(properties.getDumpDir()).mkdirs();
			FileUtils.deleteQuietly(new File(properties.getCdxDir()));
			new File(properties.getCdxDir()).mkdirs();
			
			seedsFile = new File(properties.getMetadataDir(), OnbWarcConstants.CONVENTION_SEEDS_FILE);

			seeds = readSeedsFromFile(seedsFile);
			
			if (seeds == null) {
				return false;
			}
		} catch (Exception e) {
			log.error(e.getMessage());
			return false;
		}

		return true;
	}
	
	public void work() {
		try {
			log.info("readConfig");
			readConfig();
			if (init()) {
				log.info("rewrite");
				rewrite();
				log.info("rewrite finished");
			}
			log.info("finished");
		}
		catch(Exception e) {
			log.error(e.getMessage());
		}
		finally {
			HTTPRemoteFileRegistry.getInstance().cleanup();
		}
	}
}
