#!/bin/bash
export INSTALLDIR=/mnt/crawlerspace/testcrawler4/nas/ONBTEST
#cd $INSTALLDIR
export CLASSPATH=$INSTALLDIR/lib/netarchivesuite-harvester-core.jar:$INSTALLDIR/lib/netarchivesuite-archive-core.jar:$INSTALLDIR/lib/netarchivesuite-dk.netarkivet.monitor.jar:$INSTALLDIR/lib/netarchivesuite-wayback-indexer.jar:$INSTALLDIR/lib/onbtools-7.4.4.jar

java -Dorg.apache.commons.logging.LogFactory=org.apache.commons.logging.impl.LogFactoryImpl dk.netarkivet.onbtools.browsertrix.BrowsertrixWarcRewriter BrowsertrixWarcRewriter.properties

