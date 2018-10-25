package gr.athena.innovation.fagi.merger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author nkarag
 */
public class Configuration {

    private static final Logger LOG = LogManager.getLogger(Configuration.class);

    private static Configuration configuration;
    private int partitions;
    private EnumFusionMode fusionMode;
    private String unlinkedA;
    private String datasetA;
    private String unlinkedB;
    private String datasetB;
    private String inputDir;
    private String outputDir;

    @Override
    public String toString() {
        return "\nConfiguration{" + "\n\tpartitions=" + partitions + "\n\tfusionMode=" + fusionMode 
                + "\n\tinputDir=" + inputDir + "\n\toutputDir=" + outputDir + "\n}";
    }

    private Configuration() {
    }

    public static Configuration getInstance() {
        //lazy init
        if (configuration == null) {
            configuration = new Configuration();
        }

        return configuration;
    }

    public String getInputDir() {
        return inputDir;
    }

    public void setInputDir(String inputDir) {
        this.inputDir = inputDir;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }

    public EnumFusionMode getFusionMode() {
        return fusionMode;
    }

    public void setFusionMode(EnumFusionMode fusionMode) {
        this.fusionMode = fusionMode;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public String getUnlinkedA() {
        return unlinkedA;
    }

    public void setUnlinkedA(String unlinkedA) {
        this.unlinkedA = unlinkedA;
    }

    public String getUnlinkedB() {
        return unlinkedB;
    }

    public void setUnlinkedB(String unlinkedB) {
        this.unlinkedB = unlinkedB;
    }

    public String getDatasetA() {
        return datasetA;
    }

    public void setDatasetA(String datasetA) {
        this.datasetA = datasetA;
    }

    public String getDatasetB() {
        return datasetB;
    }

    public void setDatasetB(String datasetB) {
        this.datasetB = datasetB;
    }
}
