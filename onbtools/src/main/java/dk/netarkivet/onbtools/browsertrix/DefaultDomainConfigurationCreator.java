package dk.netarkivet.onbtools.browsertrix;

import java.util.ArrayList;

import dk.netarkivet.harvester.datamodel.DomainConfiguration;
import dk.netarkivet.harvester.datamodel.DomainHistory;
import dk.netarkivet.harvester.datamodel.HarvestChannel;
import dk.netarkivet.harvester.datamodel.Password;
import dk.netarkivet.harvester.datamodel.SeedList;

public class DefaultDomainConfigurationCreator {
	public static final HarvestChannel HIGHPRIORITY_CHANNEL = new HarvestChannel("HIGHPRIORITY", false, true, "");
	
	public static DomainConfiguration createDefaultDomainConfiguration(String aConfigname, String aDomainname) {
        SeedList seedList = new SeedList(aConfigname, aDomainname);
        ArrayList<SeedList> seedlists = new ArrayList<SeedList>();
        seedlists.add(seedList);
        
        DomainConfiguration domainConfiguration = new DomainConfiguration(
        		aConfigname, aDomainname, new DomainHistory(),
                new ArrayList<String>(), seedlists, new ArrayList<Password>());
        domainConfiguration.setOrderXmlName(OrderXmlBuilder.DEFAULT_ORDER_XML_NAME);
        return domainConfiguration;
    }
}
