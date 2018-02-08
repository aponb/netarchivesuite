/*
 * #%L
 * Netarchivesuite - harvester
 * %%
 * Copyright (C) 2005 - 2017 The Royal Danish Library, 
 *             the National Library of France and the Austrian National Library.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 2.1 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 * 
 * You should have received a copy of the GNU General Lesser Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-2.1.html>.
 * #L%
 */
package dk.netarkivet.harvester.datamodel;

import java.util.Date;
import java.util.Iterator;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.netarkivet.common.exceptions.ArgumentNotValid;
import dk.netarkivet.common.exceptions.IOFailure;
import dk.netarkivet.common.exceptions.UnknownID;
import dk.netarkivet.common.utils.FilterIterator;
import dk.netarkivet.harvester.datamodel.extendedfield.ExtendedFieldDAO;

/**
 * This class contains the specific properties and operations of snapshot harvest definitions.
 */
public class FullHarvest extends HarvestDefinition {

    /** The class logger. */
    private static final Logger log = LoggerFactory.getLogger(FullHarvest.class);

    /** The maximum number of objects retrieved from each domain during a snapshot harvest. */
    private long maxCountObjects;

    /** The maximum number of bytes retrieved from each domain during a snapshot harvest. */
    private long maxBytes;

    /** The maximum time in seconds to run for each job generated by this definition. */
    private long maxJobRunningTime;

    /** The ID for the harvestdefinition, this FullHarvest is based upon. */
    private Long previousHarvestDefinitionOid;

    /** a boolean to indicate whether the deduplication index is ready. */
    private boolean indexReady;

    private final Provider<HarvestDefinitionDAO> hdDaoProvider;
    private final Provider<JobDAO> jobDaoProvider; // Not used
    private final Provider<DomainDAO> domainDAOProvider;

    /**
     * Create new instance of FullHarvest configured according to the properties of the supplied DomainConfiguration.
     * Should only be used by the HarvestFactory class.
     *
     * @param harvestDefName the name of the harvest definition
     * @param comments comments
     * @param previousHarvestDefinitionOid This harvestDefinition is used to create this Fullharvest definition.
     * @param maxCountObjects Limit for how many objects can be fetched per domain
     * @param maxBytes Limit for how many bytes can be fetched per domain
     * @param maxJobRunningTime Limit on how much time can be spent on each job. 0 means no limit
     * @param isIndexReady Is the deduplication index ready for this harvest.
     */
    public FullHarvest(String harvestDefName, String comments, Long previousHarvestDefinitionOid, long maxCountObjects,
            long maxBytes, long maxJobRunningTime, boolean isIndexReady, Provider<HarvestDefinitionDAO> hdDaoProvider,
            Provider<JobDAO> jobDaoProvider, Provider<ExtendedFieldDAO> extendedFieldDAOProvide,
            Provider<DomainDAO> domainDAOProvider) {
        super(extendedFieldDAOProvide);
        ArgumentNotValid.checkNotNullOrEmpty(harvestDefName, "harvestDefName");
        ArgumentNotValid.checkNotNull(comments, "comments");
        this.previousHarvestDefinitionOid = previousHarvestDefinitionOid;
        this.harvestDefName = harvestDefName;
        this.comments = comments;
        this.maxCountObjects = maxCountObjects;
        this.numEvents = 0;
        this.maxBytes = maxBytes;
        this.maxJobRunningTime = maxJobRunningTime;
        this.indexReady = isIndexReady;
        this.hdDaoProvider = hdDaoProvider;
        this.jobDaoProvider = jobDaoProvider;
        this.domainDAOProvider = domainDAOProvider;
    }

    /**
     * Get the previous HarvestDefinition which is used to base this.
     *
     * @return The previous HarvestDefinition
     */
    public HarvestDefinition getPreviousHarvestDefinition() {
        if (previousHarvestDefinitionOid != null) {
            return hdDaoProvider.get().read(previousHarvestDefinitionOid);
        }
        return null;
    }

    /**
     * Set the previous HarvestDefinition which is used to base this.
     *
     * @param prev The id of a HarvestDefinition
     */
    public void setPreviousHarvestDefinition(Long prev) {
        previousHarvestDefinitionOid = prev;
    }

    /** @return Returns the maxCountObjects. */
    public long getMaxCountObjects() {
        return maxCountObjects;
    }

    /** @param maxCountObjects The maxCountObjects to set. */
    public void setMaxCountObjects(long maxCountObjects) {
        this.maxCountObjects = maxCountObjects;
    }

    /**
     * Get the maximum number of bytes that this fullharvest will harvest per domain, 0 for no limit.
     *
     * @return Total download limit in bytes per domain.
     */
    public long getMaxBytes() {
        return maxBytes;
    }

    /**
     * Set the limit for how many bytes this fullharvest will harvest per domain, or -1 for no limit.
     *
     * @param maxBytes Number of bytes to stop harvesting at.
     */
    public void setMaxBytes(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    /**
     * Returns an iterator of domain configurations for this harvest definition. Domains are filtered out if, on the
     * previous harvest, they: 1) were completed 2) reached their maxBytes limit (and the maxBytes limit has not changed
     * since time of harvest) 3) reached their maxObjects limit (and the maxObjects limit has not changed since time of
     * harvest) 4) died uncleanly (e.g. due to a manual shutdown of heritrix) on their last harvest.
     * <p>
     * Domains are also excluded if they are aliases of another domain.
     *
     * @return Iterator containing information about the domain configurations
     */
    public Iterator<DomainConfiguration> getDomainConfigurations() {
        if (previousHarvestDefinitionOid == null) {
            // The first snapshot harvest
            return hdDaoProvider.get().getSnapShotConfigurations();
        } else { // An iterative snapshot harvest
            return getDomainConfigurationsForIterativeHarvest();
        }
    }
    
    /**
     * @return a iterator of DomainConfigurations not finished in previous SnapShot harvest  
     */
    public Iterator<DomainConfiguration> getDomainConfigurationsForIterativeHarvest() {
        final DomainDAO dao = domainDAOProvider.get();
        final HarvestDefinition hd = getPreviousHarvestDefinition();
        log.debug("Retrieving a list of domainconfigurations to continue SnapshotHarvest HD #{}({}) in HD #{} ({})", hd.getOid(), hd.getName(), getOid(), getName() );
        Iterator<Domain> j = dao.getAllDomainsInSnapshotHarvestOrder();
        
        return new FilterIterator<Domain, DomainConfiguration>(j) {

            @Override
            protected DomainConfiguration filter(Domain d) {
                HarvestInfo harvestInfo = dao.getHarvestInfoForDomainInHarvest(hd, d);
                if (harvestInfo == null) { // Domain not found in HarvestInfo
                    return null;
                }
                log.debug("Found harvestInfo for domain '{}'", d.getName());
                if (harvestInfo.getStopReason() == StopReason.DOWNLOAD_COMPLETE
                        || harvestInfo.getStopReason() == StopReason.DOWNLOAD_UNFINISHED) {
                    // Don't include the ones that finished or died
                    // in an unclean fashion
                    return null;
                }
                DomainConfiguration config = getConfigurationFromPreviousHarvest(harvestInfo, dao);
                // Check if max_bytes was reached
                if (harvestInfo.getStopReason() == StopReason.CONFIG_SIZE_LIMIT) {
                    // Check if MaxBytes limit for DomainConfiguration have
                    // been raised since previous harvest.
                    // If this is the case, return the configuration
                    int compare = NumberUtils.compareInf(config.getMaxBytes(), harvestInfo.getSizeDataRetrieved());
                    if (compare < 1) {
                        return null;
                    } else {
                        return config;
                    }
                }
                if (harvestInfo.getStopReason() == StopReason.CONFIG_OBJECT_LIMIT) {
                    // Check if MaxObjects limit for DomainConfiguration have
                    // been raised since previous harvest.
                    // If this is the case, return the configuration
                    int compare = NumberUtils.compareInf(config.getMaxObjects(), harvestInfo.getCountObjectRetrieved());
                    if (compare < 1) {
                        return null;
                    } else {
                        return config;
                    }
                }
                if (d.getAliasInfo() != null && !d.getAliasInfo().isExpired()) {
                    // Don't include aliases
                    return null;
                } else {
                    return config;
                }
            }
        };
    }
 
    /**
     * Get the configuration used in a previous harvest. If the configuration in the harvestinfo cannot be found
     * (deleted), uses the default configuration.
     *
     * @param harvestInfo A harvest info object from a previous harvest.
     * @param dao The dao to read configurations from.
     * @return A configuration if found and the download in this harvestinfo was complete, null otherwise
     */
    private DomainConfiguration getConfigurationFromPreviousHarvest(final HarvestInfo harvestInfo, DomainDAO dao) {
        // For each bit of harvest info that did not complete
        try {
            Domain domain = dao.read(harvestInfo.getDomainName());
            // Read the domain
            DomainConfiguration configuration;
            // Read the configuration
            try {
                configuration = domain.getConfiguration(harvestInfo.getDomainConfigurationName());
            } catch (UnknownID e) {
                // If the old configuration cannot be found, fall
                // back on default configuration
                configuration = domain.getDefaultConfiguration();
                log.debug(
                        "Previous configuration '{}' for harvesting domain '{}' not found. Using default '{}' instead.",
                        harvestInfo.getDomainConfigurationName(), harvestInfo.getDomainName(), configuration.getName(),
                        e);
            }
            // Add the configuration to the list to harvest
            return configuration;
        } catch (UnknownID e) {
            // If the domain doesn't exist, warn
            log.debug("Previously harvested domain '{}' no longer exists. Ignoring this domain.",
                    harvestInfo.getDomainName(), e);
        } catch (IOFailure e) {
            // If the domain can't be read, warn
            log.debug("Previously harvested domain '{}' can't be read. Ignoring this domain.",
                    harvestInfo.getDomainName(), e);
        }
        return null;
    }

    /**
     * Check if this harvest definition should be run, given the time now.
     *
     * @param now The current time
     * @return true if harvest definition should be run
     */
    public boolean runNow(Date now) {
        return getActive() && (numEvents < 1);
    }

    /**
     * Returns whether this HarvestDefinition represents a snapshot harvest.
     *
     * @return Returns true
     */
    public boolean isSnapShot() {
        return true;
    }

    /**
     * @return Returns the max job running time
     */
    public long getMaxJobRunningTime() {
        return maxJobRunningTime;
    }

    /**
     * Set the limit for how many seconds each crawljob in this fullharvest will run, or 0 for no limit.
     *
     * @param maxJobRunningtime max number of seconds
     */
    public void setMaxJobRunningTime(long maxJobRunningtime) {
        this.maxJobRunningTime = maxJobRunningtime;
    }

    /**
     * Is index ready. Used to check, whether or a FullHarvest is ready for scheduling. The scheduling requires, that
     * the deduplication index used by the jobs in the FullHarvest, has already been prepared by the IndexServer.
     *
     * @return true, if the deduplication index is ready. Otherwise false.
     */
    public boolean getIndexReady() {
        return this.indexReady;
    }

    /**
     * Set the indexReady field.
     *
     * @param isIndexReady The new value of the indexReady field.
     */
    public void setIndexReady(boolean isIndexReady) {
        this.indexReady = isIndexReady;
    }

}
