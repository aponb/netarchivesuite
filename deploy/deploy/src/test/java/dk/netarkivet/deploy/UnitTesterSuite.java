/* File:    $Id$
 * Version: $Revision$
 * Date:    $Date$
 * Author:  $Author$
 *
 * The Netarchive Suite - Software to harvest and preserve websites
 * Copyright 2004-2012 The Royal Danish Library, the Danish State and
 * University Library, the National Library of France and the Austrian
 * National Library.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */

package dk.netarkivet;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import dk.netarkivet.archive.arcrepository.ArchiveArcRepositoryTesterSuite;
import dk.netarkivet.archive.arcrepository.bitpreservation.ArchiveArcrepositoryBitPreservationTesterSuite;
import dk.netarkivet.archive.arcrepository.distribute.ArchiveArcrepositoryDistributeTesterSuite;
import dk.netarkivet.archive.arcrepositoryadmin.ArchiveArcRepositoryAdminTesterSuite;
import dk.netarkivet.archive.bitarchive.ArchiveBitarchiveTesterSuite;
import dk.netarkivet.archive.bitarchive.distribute.ArchiveBitarchiveDistributeTesterSuite;
import dk.netarkivet.archive.checksum.ArchiveChecksumTesterSuite;
import dk.netarkivet.archive.checksum.distribute.ArchiveChecksumDistributeTesterSuite;
import dk.netarkivet.archive.distribute.ArchiveDistributeTesterSuite;
import dk.netarkivet.archive.indexserver.ArchiveIndexServerTesterSuite;
import dk.netarkivet.archive.indexserver.distribute.ArchiveIndexserverDistributeTesterSuite;
import dk.netarkivet.archive.tools.ArchiveToolsTesterSuite;
import dk.netarkivet.archive.webinterface.ArchiveWebinterfaceTesterSuite;
import dk.netarkivet.common.CleanupSuite;
import dk.netarkivet.common.CommonsTesterSuite;
import dk.netarkivet.common.distribute.CommonDistributeTesterSuite;
import dk.netarkivet.common.distribute.arcrepository.CommonDistributeArcrepositoryTesterSuite;
import dk.netarkivet.common.distribute.indexserver.CommonDistributeIndexserverTesterSuite;
import dk.netarkivet.common.exceptions.CommonExceptionsTesterSuite;
import dk.netarkivet.common.lifecycle.CommonLifecycleTesterSuite;
import dk.netarkivet.common.management.CommonManagementTesterSuite;
import dk.netarkivet.common.tools.CommonToolsTesterSuite;
import dk.netarkivet.common.utils.CommonUtilsTesterSuite;
import dk.netarkivet.common.utils.arc.CommonUtilsArcTesterSuite;
import dk.netarkivet.common.utils.batch.CommonUtilsBatchTesterSuite;
import dk.netarkivet.common.utils.cdx.CommonUtilsCdxTesterSuite;
import dk.netarkivet.common.webinterface.CommonWebinterfaceTesterSuite;
import dk.netarkivet.deploy.DeployTesterSuite;
import dk.netarkivet.harvester.HarvesterTesterSuite;
import dk.netarkivet.harvester.datamodel.HarvesterDataModelTesterSuite;
import dk.netarkivet.harvester.datamodel.extendedfield.HarvesterDataModelExtendedfieldTesterSuite;
import dk.netarkivet.harvester.distribute.HarvesterDistributeTesterSuite;
import dk.netarkivet.harvester.harvesting.HarvestingTesterSuite;
import dk.netarkivet.harvester.harvesting.distribute.HarvestingDistributeTesterSuite;
import dk.netarkivet.harvester.harvesting.frontier.HarvesterHarvestingFrontierTesterSuite;
import dk.netarkivet.harvester.scheduler.HarvesterSchedulerTesterSuite;
import dk.netarkivet.harvester.tools.HarvesterToolsTesterSuite;
import dk.netarkivet.harvester.webinterface.HarvesterWebinterfaceTesterSuite;
import dk.netarkivet.monitor.MonitorTesterSuite;
import dk.netarkivet.monitor.jmx.MonitorJMXTesterSuite;
import dk.netarkivet.monitor.logging.MonitorLoggingTesterSuite;
import dk.netarkivet.monitor.registry.MonitorRegistryTesterSuite;
import dk.netarkivet.monitor.webinterface.MonitorWebinterfaceTesterSuite;
import dk.netarkivet.viewerproxy.ViewerProxyTesterSuite;
import dk.netarkivet.viewerproxy.distribute.ViewerproxyDistributeTesterSuite;
import dk.netarkivet.viewerproxy.webinterface.ViewerproxyWebinterfaceTesterSuite;
import dk.netarkivet.wayback.WaybackTesterSuite;

/**
 * This class runs all the unit tests.
 */
public class UnitTesterSuite {
    public static void addToSuite(TestSuite suite) {
        // Please keep sorted after module
        
        /* 
         * Testersuites for the archive module 
         */        
        ArchiveArcRepositoryTesterSuite.addToSuite(suite);
        ArchiveArcRepositoryAdminTesterSuite.addToSuite(suite);
        ArchiveArcrepositoryBitPreservationTesterSuite.addToSuite(suite);
        ArchiveArcrepositoryDistributeTesterSuite.addToSuite(suite);
        ArchiveBitarchiveTesterSuite.addToSuite(suite);
        ArchiveBitarchiveDistributeTesterSuite.addToSuite(suite);
        ArchiveChecksumTesterSuite.addToSuite(suite);
        ArchiveChecksumDistributeTesterSuite.addToSuite(suite);
        ArchiveDistributeTesterSuite.addToSuite(suite);
        ArchiveIndexserverDistributeTesterSuite.addToSuite(suite);
        ArchiveIndexServerTesterSuite.addToSuite(suite);
        ArchiveToolsTesterSuite.addToSuite(suite);
        ArchiveWebinterfaceTesterSuite.addToSuite(suite);
        
        /* 
         * Testersuites for the common module 
         */
        CommonUtilsArcTesterSuite.addToSuite(suite);
        CommonUtilsBatchTesterSuite.addToSuite(suite);
        CommonUtilsCdxTesterSuite.addToSuite(suite);
        CommonsTesterSuite.addToSuite(suite);
        CommonLifecycleTesterSuite.addToSuite(suite);
        CommonUtilsTesterSuite.addToSuite(suite);
        CommonDistributeArcrepositoryTesterSuite.addToSuite(suite);
        CommonDistributeIndexserverTesterSuite.addToSuite(suite);
        CommonDistributeTesterSuite.addToSuite(suite);
        CommonExceptionsTesterSuite.addToSuite(suite);
        CommonManagementTesterSuite.addToSuite(suite);
        CommonToolsTesterSuite.addToSuite(suite);
        CommonWebinterfaceTesterSuite.addToSuite(suite);
        
        /* 
         * Testersuites for the deploy module 
         */
        DeployTesterSuite.addToSuite(suite);
        
        /* 
         * Testersuites for the harvester module 
         */
        HarvesterTesterSuite.addToSuite(suite);
        HarvestingTesterSuite.addToSuite(suite);
        HarvesterDataModelTesterSuite.addToSuite(suite);
        HarvesterDataModelExtendedfieldTesterSuite.addToSuite(suite);
        HarvesterDistributeTesterSuite.addToSuite(suite);
        HarvesterHarvestingFrontierTesterSuite.addToSuite(suite);
        HarvestingDistributeTesterSuite.addToSuite(suite);
        HarvesterSchedulerTesterSuite.addToSuite(suite);
        HarvesterToolsTesterSuite.addToSuite(suite);
        HarvesterWebinterfaceTesterSuite.addToSuite(suite);
        
        /* 
         * Testersuites for the monitor module 
         */
        MonitorTesterSuite.addToSuite(suite);
        MonitorLoggingTesterSuite.addToSuite(suite);
        MonitorJMXTesterSuite.addToSuite(suite);
        MonitorRegistryTesterSuite.addToSuite(suite);
        MonitorWebinterfaceTesterSuite.addToSuite(suite);
        
        /* 
         * Testersuites for the viewerproxy module 
         */
        ViewerproxyDistributeTesterSuite.addToSuite(suite);
        ViewerProxyTesterSuite.addToSuite(suite);        
        ViewerproxyWebinterfaceTesterSuite.addToSuite(suite);
        
        /* 
         * Testersuites for the wayback module 
         */
        WaybackTesterSuite.addToSuite(suite);
        
        /*
         * Dummy testersuite to cleanup after the tests.
         */
        CleanupSuite.addToSuite(suite);
    }

    public static Test suite() {
        TestSuite suite;
        suite = new TestSuite(UnitTesterSuite.class.getName());

        addToSuite(suite);

        return suite;
    }

    public static void main(String[] args) {
        String[] args2 = {"-noloading", UnitTesterSuite.class.getName()};
        TestRunner.main(args2);
    }
}