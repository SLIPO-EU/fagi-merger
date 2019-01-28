package gr.athena.innovation.fagi.merger;

import java.io.File;

/**
 *
 * @author nkarag
 */
public class Configuration {

    private static Configuration configuration;
    private int partitions;
    private EnumFusionMode fusionMode;
    private String unlinkedA;
    private String datasetA;
    private String unlinkedB;
    private String datasetB;
    private String inputDir;
    private String outputDir;
    private String partialOutputDirName;
    private String fused;
    private String remaining;
    private String ambiguous;
    private String statistics;
    private String fusionLog;
    

    @Override
    public String toString() {
        return "Configuration{" + "partitions=" + partitions + ", fusionMode=" + fusionMode + ", unlinkedA=" 
                + unlinkedA + ", datasetA=" + datasetA + ", unlinkedB=" + unlinkedB + ", datasetB=" + datasetB 
                + ", inputDir=" + inputDir + ", outputDir=" + outputDir 
                + ", partialOutputDirName=" + partialOutputDirName + '}';
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

    public String getPartialOutputDirName() {
        return partialOutputDirName;
    }

    public void setPartialOutputDirName(String partialOutputDirName) {
        this.partialOutputDirName = partialOutputDirName;
    }

    public String getFused() {
        return fused;
    }

    public void setFused(String fused) throws WrongInputException {
        if(fused == null || fused.isEmpty()){
            if(outputDir == null || outputDir.isEmpty()){
                throw new WrongInputException("Define fused filepath in XML");
            }
            
            this.fused = outputDir + File.separator + Constants.FUSED;
        } else {
            this.fused = fused;
        }
    }

    public String getRemaining() {
        return remaining;
    }

    public void setRemaining(String remaining) throws WrongInputException {
        if(remaining == null || remaining.isEmpty()){
            if(outputDir == null || outputDir.isEmpty()){
                throw new WrongInputException("Define remaining filepath in XML");
            }
            
            this.remaining = outputDir + File.separator + Constants.REMAINING;
        } else {
            this.remaining = remaining;
        }
    }

    public String getAmbiguous() {
        return ambiguous;
    }

    public void setAmbiguous(String ambiguous) throws WrongInputException {
        if(ambiguous == null || ambiguous.isEmpty()){
            if(outputDir == null || outputDir.isEmpty()){
                throw new WrongInputException("Define ambiguous filepath in XML");
            }
            
            this.ambiguous = outputDir + File.separator + Constants.AMBIGUOUS;
        } else {
            this.ambiguous = ambiguous;
        }
    }

    public String getStatistics() {
        return statistics;
    }

    public void setStatistics(String statistics) throws WrongInputException {
        if(statistics == null || statistics.isEmpty()){
            if(outputDir == null || outputDir.isEmpty()){
                throw new WrongInputException("Define statistics filepath in XML");
            }
            
            this.statistics = outputDir + File.separator + Constants.STATS;
        } else {
            this.statistics = statistics;
        }
    }

    public String getFusionLog() {
        return fusionLog;
    }

    public void setFusionLog(String fusionLog) throws WrongInputException {
        if(fusionLog == null || fusionLog.isEmpty()){
            if(outputDir == null || outputDir.isEmpty()){
                throw new WrongInputException("Define fusionLog filepath in XML");
            }
            
            this.fusionLog = outputDir + File.separator + Constants.DEFAULT_FUSION_LOG_FILENAME;
        } else {
            this.fusionLog = fusionLog;
        }
    }
}
