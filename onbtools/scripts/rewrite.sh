#!/bin/bash
export INSTALLDIR=/path/to/nas
export CLASSPATH=$INSTALLDIR/lib/netarchivesuite-harvester-core.jar:$INSTALLDIR/lib/netarchivesuite-archive-core.jar:$INSTALLDIR/lib/netarchivesuite-dk.netarkivet.monitor.jar:$INSTALLDIR/lib/netarchivesuite-wayback-indexer.jar:$INSTALLDIR/lib/onbtools-7.4.4-jar-with-dependencies.jar

java -Dorg.apache.commons.logging.LogFactory=org.apache.commons.logging.impl.LogFactoryImpl -Dlog4j2.configuration=file:log4j.properties dk.netarkivet.onbtools.browsertrix.BrowsertrixWarcRewriter BrowsertrixWarcRewriter.properties

