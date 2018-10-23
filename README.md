# fagi-merger
This project is used for merging multiple results from fagi instances after using the partitioning process.
___

### Building from source
Clone the project to a preferred location:

`git clone https://github.com/SLIPO-EU/fagi-merger.git fagi-merger`

Then, go the root directory of the project (fagi-merger) and run:
`mvn clean install`

### Run fagi-merger from command line
Go to the target directory of the project and run:

`java -jar fagi-merger-SNAPSHOT.jar -config /path/to/config.xml`

### Config.xml file

Inside the resources directory of the project there is a config.template.xml file. 

`partitions` the number of partitions produced during the partitioning process.

`fusionMode` the fusion mode used for the fagi instances.

`datasetA` the path of the first dataset.

`datasetB` the path of the second dataset.

`unlinkedA` the path of the file that the non-linked entities from dataset A will be written. The file gets created if it does not exist (unlike outputDir).

`unlinkedB` the path of the file that the non-linked entities from dataset B will be written. The file gets created if it does not exist (unlike outputDir).

`inputDir` the directory path that contains the partitions.

`outputDir` the output directory path for the merged results.


