# onbtools
## Browsertrixwarcrewriter
### How to build
```
git clone https://github.com/aponb/netarchivesuite.git
cd netarchivesuite
git checkout browsertrixwarcrewriter
mvn package -Dmaven.test.skip=true
```
### Settings in BrowsertrixWarcRewriter.properties
| Key       | value
| ------------- |-----
| nasDB_USERNAME| nas username
| nasDB_PASSWORD| nas password
| nasDB_MACHINE| nas Harvest DB Servername
| nasDB_PORT| nas Harvest DB  Port
| nasDB_BASE_URL| jdbc URL for Harvest DB of Netarchivesuite e.g. jdbc:postgresql://db:5432/netarchive?searchpath=netarchivesuite
| nasDB_SPECIFICS_CLASS| e.g. dk.netarkivet.harvester.datamodel.PostgreSQLSpecifics
| nasDB_DIR| e.g. netarchive?searchpath=netarchivesuite
| jmsbroker| jms broker server name
| nasenvironmentname| nas environment name of used netarchivesuite installation e.g 7676 TEST
| nasuploadapplicationname| e.g. browsertrixupload
| nasuploadapplicationinstanceid| e.g. 1
| harvestdefinitionname| existing harvestdefinition name of used netarchivesuite installation. All generated jobs will be jobs of this harvestdefinition
| archiveDir| directory path of browsertrix warc files e.b. test/warcs
| logsDir| directory path for browsertrix logs which will be inserted in the metadata warc file
| cdxDir| directory path for cdx files, which will be generated for deduplication
| metadataDir| directory path for metadata, which describe the scope of the jobs. Mandatory file is seeds.txt in that Directory, which contains the startseeds of the jobs
| destDir| directory for the result files
| dumpDir| directory for tmp files
| dedupIndexDir| directory for dedup index (bdb files)
| dryRun| true or false - if true, then NAS will not be called and there will be files generatated starting with jobid 1 - all jobs will be part of a harvestdefinition 1
| useDeduplication| true or false - using deduplication




### How to deploy and run
  * copy netarchivesuite/onbtools/target/onbtools-7.4.4.jar to a running netarchivesuite instance
  * customize settings in scripts/BrowsertrixWarcRewriter.properties
  * customize INSTALLDIR Path in scripts/rewrite.sh
  * call scripts/rewrite.sh


