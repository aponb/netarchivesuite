package dk.netarkivet.onbtools.browsertrix;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dk.netarkivet.harvester.datamodel.Domain;
import dk.netarkivet.harvester.datamodel.DomainConfiguration;
import dk.netarkivet.harvester.datamodel.DomainDAO;
import dk.netarkivet.harvester.datamodel.H1HeritrixTemplate;
import dk.netarkivet.harvester.datamodel.H3HeritrixTemplate;
import dk.netarkivet.harvester.datamodel.HarvestDefinitionDAO;
import dk.netarkivet.harvester.datamodel.HarvestRunInfo;
import dk.netarkivet.harvester.datamodel.HeritrixTemplate;
import dk.netarkivet.harvester.datamodel.Job;
import dk.netarkivet.harvester.datamodel.JobDAO;
import dk.netarkivet.harvester.datamodel.JobStatus;
import dk.netarkivet.harvester.datamodel.PartialHarvest;
import dk.netarkivet.harvester.datamodel.Password;
import dk.netarkivet.harvester.datamodel.SeedList;
import dk.netarkivet.harvester.datamodel.SparseDomainConfiguration;
import dk.netarkivet.harvester.datamodel.eav.EAV;
import dk.netarkivet.harvester.datamodel.eav.EAV.AttributeAndType;

public class NasSeedManager {
    protected static final Log log = LogFactory.getLog(NasSeedManager.class);
	
    HarvestDefinitionDAO hddao; 
    PartialHarvest eventHarvest;
	ArrayList<DomainConfiguration> domainConfigurationList = new ArrayList<DomainConfiguration>();
	HashMap<String,Set<String>> domainSeedMap;
    
    NasSeedManagerProperties p;

	public NasSeedManager() {}
    
	public NasSeedManager(NasSeedManagerProperties aProp) {
		p = aProp;
		domainSeedMap = getDomainSeedMap(p.getSeedlist());
	}
	
	private HashMap<String,Set<String>> getDomainSeedMap(ArrayList<String> aSeedList) {
		HashMap<String,Set<String>> domainSeedMap = new HashMap<String,Set<String>>(); 
		
		if (aSeedList != null) {
		    for (int i=0; i < aSeedList.size(); i++) {
		    	String domain = null;
		    	String seed = aSeedList.get(i).trim();

		    	domain = Utils.getDomainNameFromSeed(seed);
		        
		        if (domain == null) {
		        	continue;
		        }
		        
		        Set<String> seedList = domainSeedMap.get(domain);
		        if (seedList == null) {
		        	seedList = new HashSet<String>();
		        	seedList.add(seed);
		        	domainSeedMap.put(domain, seedList);
		        }
		        else {
		        	if (!seedList.contains(seed)) {
			        	seedList.add(seed);
		        	}
		        }
		    }
		}
	    
	    return domainSeedMap;
	}
	
    public ArrayList<DomainConfiguration> getDomainConfigurationList() {
		return domainConfigurationList;
	}
	
	private boolean requirementsOk() {
		if (p == null || !p.validate()) {
			log.warn("Properties not valid");
	    	return false;
		}
		
	    if (p.getSeedlist() == null || p.getSeedlist().size() == 0) {
			log.warn("no seeds found. Nothing to do");
	    	return true;
	    }
	    
		return true;
	}

	private boolean createDomainsAndConfigs() {
	    DomainDAO domaindao = DomainDAO.getInstance(); 
	    
		Iterator<String> itDomains = domainSeedMap.keySet().iterator();
		
		domainConfigurationList = new ArrayList<DomainConfiguration>();
		
		while(itDomains.hasNext()) {
			String domainName = itDomains.next();
			log.info(domainName);
			
			Iterator<String> itDomainSeeds = domainSeedMap.get(domainName).iterator();
			
	        try {
	        	// wenn domain nicht existiert, erzeuge domain
	        	Domain domain = null;
	            if (DomainDAO.getInstance().exists(domainName)) {
	                domain = domaindao.read(domainName);
	            }
	            else {
	            	domaindao.create(Domain.getDefaultDomain(domainName));	            	
	                domain = domaindao.read(domainName);
	            }
	            
	            // wenn seedlist nicht existiert, erzeuge sie
	            Iterator<SeedList> it = domain.getAllSeedLists();
	            boolean seedlistexists = false;
	            
	            SeedList myseedlist = null;
	            while(it.hasNext()) {
	            	SeedList sl = it.next();
	            	if (sl.getName().equals(p.getSeedlistname())) {
	            		seedlistexists = true;
	            		myseedlist = sl;
	            		break;
	            	}
	            }

                if (!seedlistexists) {
                	myseedlist =  new SeedList(p.getSeedlistname(), "");
                }

	            // Seeds zur Seedlist hinzufuegen und loesche eventuelle Seeds wenn gewuenscht
                
                if (p.isClearSeedlistBeforeAdding()) {
                	myseedlist.getSeeds().clear();
                }
                
    		    while(itDomainSeeds.hasNext()) {
    		    	String seed = itDomainSeeds.next();
    		    	
    		    	boolean found = false;
    		    	for (int i=0; i < myseedlist.getSeeds().size(); i++) {
    		    		if (myseedlist.getSeeds().get(i).equals(seed) ) {
    		    			found = true;
    		    			break;
    		    		}
    		    			
    		    	}
    		    	if (found) {
    		    		continue;
    		    	}
    		    	
    		    	myseedlist.getSeeds().add(seed);
    		    }

    		    if (!seedlistexists) {
                    domain.addSeedList(myseedlist);
    		    }
    		    
    		    DomainConfiguration domainConfig = null;
    		    
                // Configuration erzeugen wenn gewünscht
                if (p.isCreateconfiguration()) {
                	// wenn configuration vorhanden, dann myseedlist zu den ausgewählten Seedlists hinzufuegen, wenn noch nicht getan
                	if (domain.hasConfiguration(p.getConfigurationname())) {
                		domainConfig = domain.getConfiguration(p.getConfigurationname());
                		
                		Iterator<SeedList> itSeedListsOfConfiguration = domainConfig.getSeedLists();
                		boolean found = false;
                		while(itSeedListsOfConfiguration.hasNext()) {
                			SeedList seedlistOfConfiguration = itSeedListsOfConfiguration.next();
        	            	if (seedlistOfConfiguration.getName().equals(p.getSeedlistname())) {
        	            		found = true;
        	            		break;
        	            	}
                		}
                		
                		if (!found) {
                			domainConfig.addSeedList(domain, myseedlist);
                		}
                	}
                	else {
                		// Neue Konfiguration erzeugen
                		List<SeedList> domainConfigSeedLists = new ArrayList<SeedList>();
                		domainConfigSeedLists.add(myseedlist);
                		
        	            domainConfig = null;
                		
		                domainConfig = new DomainConfiguration(p.getConfigurationname(), domain, domainConfigSeedLists, new ArrayList<Password>());
		                domainConfig.setOrderXmlName(p.getOrderxmlname());
		                domainConfig.setMaxBytes(p.getMaxbytes());
		                domainConfig.setMaxObjects(p.getMaxobjects());
	                    domain.addConfiguration(domainConfig);
                	}
                }
                
                domaindao.update(domain);

                if (domainConfig != null) {
                    // EAV
                    List<AttributeAndType> attributesAndTypes = EAV.getInstance().getAttributesAndTypes(EAV.DOMAIN_TREE_ID, domainConfig.getID().intValue());
                    domainConfig.setAttributesAndTypes(attributesAndTypes);
                    
                    domainConfigurationList.add(domainConfig);
                }
                
	        } catch (Exception e) {
	            log.error("Unexpected exception thrown", e);
	            return false;
	        }
		}
		
		return true;
	}

	private boolean addToharvestdefinition() {
	    if (!hddao.exists(p.getHarvestdefinitionname())) {
			log.error(p.getHarvestdefinitionname() + " not found");
	        return false;
	    }
		
        eventHarvest = (PartialHarvest) hddao.getHarvestDefinition(p.getHarvestdefinitionname());
        Collection<DomainConfiguration> list = eventHarvest.getDomainConfigurationsAsList();

        if (p.isEmptyHarvestdefinition()) {
        	// loesche alle DomainConfigurations von Harvestdefinition
            Iterator<DomainConfiguration> it = list.iterator();
        	
            while(it.hasNext()) {
            	DomainConfiguration dc = it.next();
            	
            	SparseDomainConfiguration sdc = new SparseDomainConfiguration(dc.getDomainName(), dc.getName());
            	hddao.removeDomainConfiguration(eventHarvest.getOid(), sdc);
            }
        }

		List<String> domainsInHarvestDefinition = hddao.getListOfDomainsOfHarvestDefinition(p.getHarvestdefinitionname());
		
		HashMap<String,Set<String>> domainSeedMap = getDomainSeedMap(p.getSeedlist());
		Iterator<String> itDomains = domainSeedMap.keySet().iterator();
		
		
		while(itDomains.hasNext()) {
			String domainName = itDomains.next();
			
			boolean found = false;
			
			for (int i=0; i < domainsInHarvestDefinition.size(); i++) {
				String d = domainsInHarvestDefinition.get(i);
				
				if (d.equals(domainName)) {
					found = true;
					break;
				}
			}
			
			if (found) {
				continue;
			}
			
	    	SparseDomainConfiguration sdc = new SparseDomainConfiguration(domainName, p.getConfigurationname());
	    	hddao.addDomainConfiguration(eventHarvest, sdc);
		}
		
		return true;
	}
	
	private long createFinishedJobForDomain(long aHarvestid, String aDomainname, long aNumberOfObjects, long aNumberOfBytes) {
		log.info("create job");
		
	    JobDAO jobdao = JobDAO.getInstance();

        long forceMaxObjectsPerDomain = -1, forceMaxBytesPerDomain = -1, forceMaxJobRunningTime = -1;
        int harvestNum = 1;
        HeritrixTemplate heritrixtemplate = new H1HeritrixTemplate(OrderXmlBuilder.createDefault().getDoc());
        
        Job job = new Job(aHarvestid,
        		DefaultDomainConfigurationCreator.createDefaultDomainConfiguration(p.getConfigurationname(), aDomainname),
                heritrixtemplate,
                DefaultDomainConfigurationCreator.HIGHPRIORITY_CHANNEL,
                forceMaxObjectsPerDomain,
                forceMaxBytesPerDomain,
                forceMaxJobRunningTime,
                harvestNum);
	    
	    jobdao.create(job);
	    job.setStatus(JobStatus.fromOrdinal(JobInfo.DONE));
	    job.setActualStart(new Date());
	    job.setActualStop(new Date());
	    	    
	    jobdao.update(job);
	    return job.getJobID();
	}

	private String loadDummyTemplate() {
		String template = "";
        BufferedReader br = null;
		
        try {
            br = new BufferedReader(new InputStreamReader(NasSeedManager.class.getResourceAsStream("/h3.xml")));
            String line;
            while ((line = br.readLine()) != null) {
            	template += line;
            }
            br.close();
        } catch (Exception e) {
        	log.error(e.getMessage());
        } finally {
            try {
                if(br != null) {
                    br.close();
                }
            } catch (IOException e) {
            }
        }
        return template;
	}
	
	public JobHdInfo createFinishedJobForDomainsWithSeedList() {
		if (!prepareDomains()) {
			return null;
		}
		
	    hddao = HarvestDefinitionDAO.getInstance(); 
		
	    if (!hddao.exists(p.getHarvestdefinitionname())) {
			log.error(p.getHarvestdefinitionname() + " not found");
			return null;
	    }

        eventHarvest = (PartialHarvest) hddao.getHarvestDefinition(p.getHarvestdefinitionname());
		
        Long harvestid = eventHarvest.getOid();
        
        List<HarvestRunInfo> harvestRunInfoList = hddao.getHarvestRunInfo(harvestid);
        
        int harvestNum = 1;
        if (harvestRunInfoList != null && harvestRunInfoList.size() > 0) {
        	harvestNum = harvestRunInfoList.get(0).getRunNr() + 1;
        }
        
		log.info("create job");
        
		H3HeritrixTemplate heritrixtemplate = null;
		heritrixtemplate = new H3HeritrixTemplate(1, loadDummyTemplate());
		
		Job job = null;
        long forceMaxObjectsPerDomain = -1, forceMaxBytesPerDomain = -1, forceMaxJobRunningTime = -1;
		
		for (int i=0; i < domainConfigurationList.size(); i++) {
			DomainConfiguration dc = domainConfigurationList.get(i);
			if (i == 0) {
		        job = new Job(harvestid,
		        		dc,
		                heritrixtemplate,
		                DefaultDomainConfigurationCreator.HIGHPRIORITY_CHANNEL,
		                forceMaxObjectsPerDomain,
		                forceMaxBytesPerDomain,
		                forceMaxJobRunningTime,
		                harvestNum);
				
			}
			else {
				job.addConfiguration(dc);
			}
		}
		
	    JobDAO jobdao = JobDAO.getInstance();

	    jobdao.create(job);
	    
	    job.setStatus(JobStatus.fromOrdinal(JobInfo.DONE));
	    job.setActualStart(new Date());
	    job.setActualStop(new Date());
	    jobdao.update(job);
	    
	    JobHdInfo info = new JobHdInfo(job.getJobID(), harvestid);
	    
	    return info;
	}
	
	public boolean prepareDomains() {
		if (!requirementsOk()) {
			return false;
		}
		
		if (!createDomainsAndConfigs()) {
			return false;
		}
		
		return true;
	}		
}
