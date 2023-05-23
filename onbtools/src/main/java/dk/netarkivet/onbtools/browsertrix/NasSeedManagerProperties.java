package dk.netarkivet.onbtools.browsertrix;

import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NasSeedManagerProperties {
    protected static final Log log = LogFactory.getLog(NasSeedManagerProperties.class);
	
	public ArrayList<String> getSeedlist() {
		return seedlist;
	}
	public void addSeed(String aSeed) {
		seedlist.add(aSeed);
	}
	public void setSeedlist(ArrayList<String> seedlist) {
		this.seedlist = seedlist;
	}
	public String getConfigurationname() {
		return configurationname;
	}
	public void setConfigurationname(String configurationname) {
		this.configurationname = configurationname;
	}
	public String getSeedlistname() {
		return seedlistname;
	}
	public void setSeedlistname(String seedlistname) {
		this.seedlistname = seedlistname;
	}
	public String getHarvestdefinitionname() {
		return harvestdefinitionname;
	}
	public void setHarvestdefinitionname(String harvestdefinitionname) {
		this.harvestdefinitionname = harvestdefinitionname;
	}
	public boolean isAddToHarvestdefinition() {
		return addToHarvestdefinition;
	}
	public void setAddToHarvestdefinition(boolean addToHarvestdefinition) {
		this.addToHarvestdefinition = addToHarvestdefinition;
	}
	public boolean isClearSeedlistBeforeAdding() {
		return clearSeedlistBeforeAdding;
	}
	public void setClearSeedlistBeforeAdding(boolean clearSeedlistBeforeAdding) {
		this.clearSeedlistBeforeAdding = clearSeedlistBeforeAdding;
	}
	public boolean isCreateconfiguration() {
		return createconfiguration;
	}
	public void setCreateconfiguration(boolean createconfiguration) {
		this.createconfiguration = createconfiguration;
	}
	public boolean isEmptyHarvestdefinition() {
		return emptyHarvestdefinition;
	}
	public void setEmptyHarvestdefinition(boolean emptyHarvestdefinition) {
		this.emptyHarvestdefinition = emptyHarvestdefinition;
	}
	public long getMaxbytes() {
		return maxbytes;
	}
	public void setMaxbytes(long maxbytes) {
		this.maxbytes = maxbytes;
	}
	public long getMaxobjects() {
		return maxobjects;
	}
	public void setMaxobjects(long maxobjects) {
		this.maxobjects = maxobjects;
	}
	public String getOrderxmlname() {
		return orderxmlname;
	}
	public void setOrderxmlname(String orderxmlname) {
		this.orderxmlname = orderxmlname;
	}
	public boolean isScheduleHarvestdefinition() {
		return scheduleHarvestdefinition;
	}
	public void setScheduleHarvestdefinition(boolean scheduleHarvestdefinition) {
		this.scheduleHarvestdefinition = scheduleHarvestdefinition;
	}
	public Date getScheduleDate() {
		return scheduleDate;
	}
	public void setScheduleDate(Date scheduleDate) {
		this.scheduleDate = scheduleDate;
	}
	public boolean isScheduleAfterIt() {
		return scheduleAfterIt;
	}
	public void setScheduleAfterIt(boolean scheduleAfterIt) {
		this.scheduleAfterIt = scheduleAfterIt;
	}
	
	ArrayList<String> seedlist = new ArrayList<String>();
	
	String configurationname;
	String seedlistname;
	String harvestdefinitionname;
	
	boolean addToHarvestdefinition = false;	
	boolean clearSeedlistBeforeAdding = false;
	boolean createconfiguration = false;
	boolean emptyHarvestdefinition = false;
	boolean scheduleHarvestdefinition = false;
	boolean scheduleAfterIt = false;
	
	Date scheduleDate;
	
	long maxbytes = -1;
	long maxobjects = -1;
	String orderxmlname = "default_orderxml";
	
	public boolean validate() {
		if (seedlist == null) {
			log.error("seedlist is null");
			return false;
		}
		
		log.info("addToHarvestdefinition: " + addToHarvestdefinition);
		log.info("clearSeedlistBeforeAdding: " + clearSeedlistBeforeAdding);
		log.info("createconfiguration: " + createconfiguration);
		log.info("emptyHarvestdefinition: " + emptyHarvestdefinition);
		log.info("scheduleHarvestdefinition: " + scheduleHarvestdefinition);
		log.info("scheduleAfterIt: " + scheduleAfterIt);
		
		if (seedlistname == null) {
			log.error("seedlistname is null");
			return false;
		}
		log.info("seedlistname: " + seedlistname);
		
		if (createconfiguration) {
			if (configurationname == null) {
				log.error("configurationname is null");
				return false;
			}
			if (orderxmlname == null) {
				log.error("orderxmlname is null");
				return false;
			}
			
			log.info("configurationname: " + configurationname);
			log.info("orderxmlname: " + orderxmlname);
		}
		
		if (addToHarvestdefinition) {
			if (harvestdefinitionname == null) {
				log.error("harvestdefinitionname is null");
				return false;
			}
		}
		log.info("harvestdefinitionname: " + harvestdefinitionname);

		if (scheduleHarvestdefinition) {
			if (!scheduleAfterIt && scheduleDate == null) {
				log.error("scheduleDate is null");
				return false;
			}
		}
		log.info("scheduleDate: " + scheduleDate);
		
		log.info("maxbytes: " + maxbytes);
		log.info("maxobjects: " + maxobjects);
		
		return true;
	}
}
