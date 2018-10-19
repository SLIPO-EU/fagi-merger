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
    private String inputDir;
    private String outputDir;

    @Override
    public String toString() {
        return "Configuration{" + "partitions=" + partitions + ", fusionMode=" + fusionMode 
                + ", inputDir=" + inputDir + ", outputDir=" + outputDir + '}';
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
}
