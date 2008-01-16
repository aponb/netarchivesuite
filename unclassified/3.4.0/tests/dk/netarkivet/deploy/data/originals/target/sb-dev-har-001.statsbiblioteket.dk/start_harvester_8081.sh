#!/bin/bash
export CLASSPATH=/home/netarkiv/UNITTEST/lib/dk.netarkivet.harvester.jar:/home/netarkiv/UNITTEST/lib/dk.netarkivet.archive.jar:/home/netarkiv/UNITTEST/lib/dk.netarkivet.viewerproxy.jar:/home/netarkiv/UNITTEST/lib/dk.netarkivet.monitor.jar:$CLASSPATH;
cd /home/netarkiv/UNITTEST
java -Xmx1536m  -Dsettings.harvester.harvesting.heritrix.guiPort=8090  -Dsettings.harvester.harvesting.heritrix.jmxPort=8091 -Ddk.netarkivet.settings.file=/home/netarkiv/UNITTEST/conf/settings_harvester_8081.xml -Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.Jdk14Logger -Djava.util.logging.config.file=/home/netarkiv/UNITTEST/conf/log_harvestcontrollerapplication.prop -Dsettings.common.jmx.port=8100 -Dsettings.common.jmx.rmiPort=8200 -Dsettings.common.jmx.passwordFile=/home/netarkiv/UNITTEST/conf/jmxremote.password dk.netarkivet.harvester.harvesting.HarvestControllerApplication < /dev/null > start_harvester_8081.sh.log 2>&1 &
