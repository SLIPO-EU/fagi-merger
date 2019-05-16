package gr.athena.innovation.fagi.merger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author nkarag
 */
public class MergerInstance {

    private static final org.apache.logging.log4j.Logger LOG = LogManager.getLogger(MergerInstance.class);
    
    private static final String TYPE = "type";
    private static final String LABEL = "label";
    private static final String VALUE = "value";
    private static final String ABSOLUTE = "ABSOLUTE";

    public void run(String configPath) throws WrongInputException, MergeOperationException, IOException, ParseException {

        LOG.info("Merge process started.");

        long start = System.currentTimeMillis();

        ConfigParser parser = new ConfigParser();
        Configuration config = parser.parse(configPath);

        LOG.info("Configuration: " + config.toString());

        int partitions = config.getPartitions();
        String unlinkedA = config.getUnlinkedA();
        String datasetA = config.getDatasetA();
        String unlinkedB = config.getUnlinkedB();
        String datasetB = config.getDatasetB();
        String inputDir = config.getInputDir().replaceFirst("[/]+$", "");
        String outputDir = config.getOutputDir().replaceFirst("[/]+$", "");
        String partialOutputDirName = config.getPartialOutputDirName();

        File[] partitionDirs;
        if(partialOutputDirName != null && !partialOutputDirName.isEmpty()){
            
            File inputDirectory = new File(inputDir);
            File[] dirs = inputDirectory.listFiles();
            File[] dirsUnderInput = Arrays.stream(dirs).filter(f -> f.isDirectory()).toArray(File[]::new);
            
            List<File> partialPaths = new ArrayList<>();
            for(File dir : dirsUnderInput){
                String partialOutputPath = dir.getAbsolutePath() + File.separator + partialOutputDirName;
                partialPaths.add(new File(partialOutputPath));
            }

            partitionDirs = partialPaths.toArray(new File[0]);

        } else {
            File inputDirectory = new File(inputDir);
            File[] dirs = inputDirectory.listFiles();
            partitionDirs = Arrays.stream(dirs).filter(f -> f.isDirectory()).toArray(File[]::new);
        }
        
        if (partitionDirs.length != partitions) {
            LOG.error("Partitions found do not match configuration.");
            throw new WrongInputException("Partitions found do not match configuration.");
        }

        EnumFusionMode fusionMode = config.getFusionMode();

        mergeFused(partitionDirs, outputDir, Constants.FUSED, config.getFused()); //merge all fused.nt files
        mergeDataset(partitionDirs, Constants.REMAINING, config.getRemaining()); //merge remainint.nt files
        mergeDataset(partitionDirs, Constants.AMBIGUOUS, config.getAmbiguous()); //merge ambiguous.nt files
        mergeDataset(partitionDirs, Constants.FUSION_LOG, config.getFusionLog()); //merge fusionLog.txt files

        combineProperties(partitionDirs, config.getFusionProperties(), Constants.FUSION_PROPERTIES); //merge fusion.properties files
        combineStatistics(partitionDirs, Constants.STATS, config.getStatistics()); //merge stats.json files

        switch (fusionMode) {
            case L_MODE:
                //do nothing
                break;
            case AA_MODE:
                //combine fused with unlinked from A
                //copy dataset B as is to remaining
                combineFused(config.getFused(), unlinkedA);
                copyRemaining(datasetB, config.getRemaining());
                break;
            case BB_MODE:
                //combine fused with unlinked from B
                //copy dataset A as is to remaining
                combineFused(config.getFused(), unlinkedB);
                copyRemaining(datasetA, config.getRemaining());
                break;
            case AB_MODE:
                //combine fused with unlinked from A and unlinked from B.
                //copy dataset B as is to remaining
                combineFused(config.getFused(), unlinkedA);
                combineFused(config.getFused(), unlinkedB);
                copyRemaining(datasetB, config.getRemaining());
                break;
            case BA_MODE:
                //combine fused with unlinked from A and unlinked from B.
                //copy dataset A as is to remaining
                combineFused(config.getFused(), unlinkedB);
                combineFused(config.getFused(), unlinkedA);
                copyRemaining(datasetA, config.getRemaining());
                break;
            case A_MODE:
                //combine fused with unlinked from A.
                //copy unlinked B as is to remaining
                combineFused(config.getFused(), unlinkedA);
                copyRemaining(unlinkedB, config.getRemaining());
                break;
            case B_MODE:
                //combine fused with unlinked from B.
                //copy unlinked A as is to remaining
                combineFused(config.getFused(), unlinkedB);
                copyRemaining(unlinkedA, config.getRemaining());
                break;
        }

        long end = System.currentTimeMillis();

        LOG.info("Merge process complete.");
        String time = getFormattedTime(end - start);
        LOG.info("Time passed: " + time + ".");
    }

    private void mergeFused(File[] partitionDirs, String outputDir, String datasetId, String path) 
            throws WrongInputException, MergeOperationException {

        List<String> filePaths = new ArrayList<>();
        for (File partitionDir : partitionDirs) {
            addFilePaths(partitionDir, datasetId, filePaths);
        }

        try {

            //String outputFilepath = outputDir + File.separator + datasetId + Constants.TEMP;
            String outputFilepath = path + Constants.TEMP;
            merge(filePaths, outputFilepath);

        } catch (IOException ex) {
            LOG.error(ex);
            throw new MergeOperationException(ex.getMessage());
        }
    }

    private void addFilePaths(File partitionDir, String datasetId, List<String> filePaths) {
        File[] files = partitionDir.listFiles();
        for (File file : files) {
            if (file.getName().contains(datasetId)) {
                filePaths.add(file.getAbsolutePath());
            }
        }
    }

    private void mergeDataset(File[] partitionDirs, String datasetId, String outputPath) 
            throws WrongInputException, MergeOperationException {

        List<String> filePaths = new ArrayList<>();
        for (File partitionDir : partitionDirs) {
            File[] files = partitionDir.listFiles();
            for (File file : files) {
                if (file.getName().contains(datasetId)) {
                    filePaths.add(file.getAbsolutePath());
                }
            }
        }

        try {
            merge(filePaths, outputPath);

        } catch (IOException ex) {
            LOG.error(ex);
            throw new MergeOperationException(ex.getMessage());
        }
    }

    private void combineProperties(File[] partitionDirs, String outputPath, String propsId)
            throws WrongInputException, MergeOperationException {

        List<String> filePaths = new ArrayList<>();
        for (File partitionDir : partitionDirs) {
            File[] files = partitionDir.listFiles();
            for (File file : files) {
                if (file.getName().contains(propsId)) {
                    filePaths.add(file.getAbsolutePath());
                }
            }
        }

        try {
            combineProps(filePaths, outputPath);
        } catch (IOException ex) {
            LOG.error(ex);
            throw new MergeOperationException(ex.getMessage());
        }
    }

    private void combineStatistics(File[] partitionDirs, String statsId, String outputPath)
            throws WrongInputException, MergeOperationException, ParseException {

        List<String> filePaths = new ArrayList<>();
        for (File partitionDir : partitionDirs) {
            File[] files = partitionDir.listFiles();
            for (File file : files) {
                if (file.getName().contains(statsId)) {
                    filePaths.add(file.getAbsolutePath());
                }
            }
        }

        try {
            combineStats(filePaths, outputPath);
        } catch (IOException ex) {
            LOG.error(ex);
            throw new MergeOperationException(ex.getMessage());
        }
    }

    private void merge(List<String> paths, String outputFilename) throws IOException {

        Path outFile = Paths.get(outputFilename);

        try (FileChannel out = FileChannel.open(outFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            for (String path : paths) {
                Path inFile = Paths.get(path);
                try (FileChannel in = FileChannel.open(inFile, StandardOpenOption.READ)) {
                    for (long p = 0, l = in.size(); p < l;) {
                        p += in.transferTo(p, l - p, out);
                    }
                }
            }
        }
        LOG.info("Merge of " + outputFilename + " complete.");
    }

    private void combineProps(List<String> paths, String outputFilename) throws IOException {

        Path outFile = Paths.get(outputFilename);
        OutputStream out = Files.newOutputStream(outFile);
        Properties combinedProperties = new Properties();
        combinedProperties.setProperty(Constants.FUSED_PROPERTY, Constants.ZERO_VALUE);
        combinedProperties.setProperty(Constants.REJECTED_PROPERTY, Constants.ZERO_VALUE);

        for (String path : paths) {

            Properties currentProperty = new Properties();
            InputStream input = new FileInputStream(path);
            currentProperty.load(input);

            String fused = combinedProperties.getProperty(Constants.FUSED_PROPERTY);
            String tempFused = currentProperty.getProperty(Constants.FUSED_PROPERTY);
            Double fusedValue = Double.parseDouble(fused);
            Double tempFusedValue = Double.parseDouble(tempFused);
            Double combinedFused = fusedValue + tempFusedValue;

            combinedProperties.setProperty(Constants.FUSED_PROPERTY, combinedFused.toString());

            String rejected = combinedProperties.getProperty(Constants.REJECTED_PROPERTY);
            String tempRejected = currentProperty.getProperty(Constants.REJECTED_PROPERTY);
            Double rejectedValue = Double.parseDouble(rejected);
            Double tempRejectedValue = Double.parseDouble(tempRejected);
            Double combinedRejected = rejectedValue + tempRejectedValue;

            combinedProperties.setProperty(Constants.REJECTED_PROPERTY, combinedRejected.toString());

        }

        combinedProperties.store(out, null);
        LOG.info(Constants.FUSION_PROPERTIES + " combined.");
    }

    private void combineStats(List<String> paths, String outputFilename) throws IOException, ParseException {

        JSONParser parser = new JSONParser();
        JSONObject combinedStatsJson = null;// = new JSONObject();

        for (String path : paths) {

            JSONObject currentStatJson = (JSONObject) parser.parse(new FileReader(path));

            if (combinedStatsJson == null) {
                combinedStatsJson = currentStatJson;
                //skip this iteration to avoid double combineProps of the first stats.
                continue;
            }

            Set<Entry<String, JSONObject>> currentStats = currentStatJson.entrySet();

            for (Entry<String, JSONObject> currentStat : currentStats) {
                String statKey = currentStat.getKey();

                if(currentStat.getValue().get(Constants.Stats.TYPE) == null){
                    continue;
                }

                if (currentStat.getValue().get(Constants.Stats.TYPE).equals(Constants.Stats.UNDEFINED)) {
                    //skip calculation of stat. (The stat will be present at the merged file as undefined)
                    continue;
                }

                if (statKey.contains(Constants.Stats.Keys.PERCENT)) {
                    combinePercentStat(currentStat, combinedStatsJson);
                } else if (statKey.contains(Constants.Stats.Keys.FULL)) {
                    combineFullMatchingStat(currentStat, combinedStatsJson);
                } else if (statKey.contains(Constants.Stats.Keys.LONGER)) {
                    combineFullMatchingStat(currentStat, combinedStatsJson);
                } else if (statKey.equals(Constants.Stats.Keys.LINKED_POIS)) {
                    combineCustomStat(currentStat, combinedStatsJson);
                } else if (statKey.equals(Constants.Stats.Keys.TOTAL_EMPTY_PROPERTIES)) {
                    combineCustomStat(currentStat, combinedStatsJson);
                } else if (statKey.equals(Constants.Stats.Keys.TOTAL_NON_EMPTY_PROPERTIES)) {
                    combineCustomStat(currentStat, combinedStatsJson);
                } else if (statKey.equals(Constants.Stats.Keys.LINKED_VS_TOTAL)) {
                    combineCustomStat(currentStat, combinedStatsJson);
                } else if (statKey.equals(Constants.Stats.Keys.LINKED_TRIPLES)) {
                    combineCustomStat(currentStat, combinedStatsJson);
                } else {
                    //used for "detailed" statistics mode
                    //combineDefaultStat(currentStat, combinedStatsJson);
                    //used for "light" statistics mode
                    combineLightStat(currentStat, combinedStatsJson);
                }
            }
        }

        try (FileWriter file = new FileWriter(outputFilename)) {
            if (combinedStatsJson != null) {
                file.write(combinedStatsJson.toJSONString());
                LOG.info(Constants.STATS + " combined.");
            }
        }
    }

    private void combineDefaultStat(Entry<String, JSONObject> currentStat, JSONObject combinedStatsJson)
            throws NumberFormatException {

        JSONObject stat = currentStat.getValue();
        String statValueA = null;
        String statValueB = null;
        String statTotal = null;

        if (stat.get(Constants.Stats.VALUE_A) != null && stat.get(Constants.Stats.VALUE_B) != null 
                && stat.get(Constants.Stats.VALUE_TOTAL) != null) {
            statValueA = stat.get(Constants.Stats.VALUE_A).toString();
            statValueB = stat.get(Constants.Stats.VALUE_B).toString();
            statTotal = stat.get(Constants.Stats.VALUE_TOTAL).toString();
        }

        JSONObject combined = (JSONObject) combinedStatsJson.get(currentStat.getKey());

        String combinedValueA = (String) combined.get(Constants.Stats.VALUE_A);
        String combinedValueB = (String) combined.get(Constants.Stats.VALUE_B);

        if (combinedValueA != null && combinedValueB != null && statTotal != null) {
            Double valA = Double.parseDouble(combinedValueA);
            Double valB = Double.parseDouble(combinedValueB);
            Double total = valA + valB;

            Double oldValA = Double.parseDouble(statValueA);
            Double oldValB = Double.parseDouble(statValueB);
            Double oldTotal = Double.parseDouble(statTotal);
            Double cValA = oldValA + valA;
            Double cValB = oldValB + valB;
            Double cValTotal = oldTotal + total;

            combined.put(Constants.Stats.VALUE_A, cValA.toString());
            combined.put(Constants.Stats.VALUE_B, cValB.toString());
            combined.put(Constants.Stats.VALUE_TOTAL, cValTotal.toString());

        }

        combinedStatsJson.put(currentStat.getKey(), combined);
    }

    private void combineLightStat(Entry<String, JSONObject> currentStat, JSONObject combinedStatsJson)
            throws NumberFormatException {

        //these stas cannot be calculated in distributed mode.
        if(currentStat.getKey().equals("executionTime") 
                || currentStat.getKey().equals("averageGain") 
                ||currentStat.getKey().equals("maxGain")
                ||currentStat.getKey().equals("averageConfidence")){

            return;
        }

        JSONObject stat = currentStat.getValue();
        String rootLabel = stat.get(LABEL).toString();
        JSONArray array = (JSONArray) stat.get(Constants.Stats.ITEMS);
        
        if(array.size() > 1){
            JSONObject combined = (JSONObject) combinedStatsJson.get(currentStat.getKey());
            
            JSONArray old = (JSONArray) combined.get(Constants.Stats.ITEMS);
            JSONObject combJson1 = (JSONObject) old.get(0);
            JSONObject combJson2 = (JSONObject) old.get(1);
            
            String label1 = combJson1.get(Constants.Stats.LABEL).toString();
            String oldValue1 = combJson1.get(Constants.Stats.VALUE).toString();

            String label2 = combJson2.get(Constants.Stats.LABEL).toString();
            String oldValue2 = combJson2.get(Constants.Stats.VALUE).toString();

            if (array.get(0) != null && array.get(1) != null) {
                JSONObject json1 = (JSONObject) array.get(0);
                String currentValue1 = json1.get(Constants.Stats.VALUE).toString();
                JSONObject json2 = (JSONObject) array.get(1);
                String currentValue2 = json2.get(Constants.Stats.VALUE).toString();

                Integer valA = Integer.parseInt(currentValue1);
                Integer oldValA = Integer.parseInt(oldValue1);

                Integer cValA = oldValA + valA;
                
                Integer valB = Integer.parseInt(currentValue2);
                Integer oldValB = Integer.parseInt(oldValue2);

                Integer cValB = oldValB + valB;
                
                String updatedValue1 = cValA.toString();
                String updatedValue2 = cValB.toString();

                JSONObject up = createLightStat2(rootLabel, label1, label2, updatedValue1, updatedValue2);
                
                combinedStatsJson.put(currentStat.getKey(), up);

            }
        } else {

            JSONObject combined = (JSONObject) combinedStatsJson.get(currentStat.getKey());
            
            JSONArray old = (JSONArray) combined.get(Constants.Stats.ITEMS);
            JSONObject combJson = (JSONObject) old.get(0);
            String label = combJson.get(Constants.Stats.LABEL).toString();
            String oldValue = combJson.get(Constants.Stats.VALUE).toString();

            if (array.get(0) != null) {
                JSONObject json = (JSONObject) array.get(0);
                String currentValue = json.get(Constants.Stats.VALUE).toString();

                Integer valA = Integer.parseInt(currentValue);
                Integer oldValA = Integer.parseInt(oldValue);

                Integer cValA = oldValA + valA;
                String updatedValue = cValA.toString();

                combinedStatsJson.put(currentStat.getKey(), createLightStat1(rootLabel, label, updatedValue));

            }
        }
    }

    private JSONObject createLightStat1(String rootLabel, String label, String value) {
        JSONObject json = new JSONObject();
        json.put(TYPE, ABSOLUTE);
        json.put(LABEL, rootLabel);
        JSONArray  jsonArray = new JSONArray();

        JSONObject item = new JSONObject();
        item.put(LABEL, label);
        item.put(VALUE, value);

        jsonArray.add(item);
        json.put(Constants.Stats.ITEMS, jsonArray);
        return json;
    }

    private JSONObject createLightStat2(String rootLabel, String label1, String label2, String value1, String value2) {
        JSONObject json = new JSONObject();
        json.put(TYPE, ABSOLUTE);
        json.put(LABEL, rootLabel);
        JSONArray  jsonArray = new JSONArray();

        JSONObject item1 = new JSONObject();
        item1.put(LABEL, label1);
        item1.put(VALUE, value1);

        JSONObject item2 = new JSONObject();
        item2.put(LABEL, label2);
        item2.put(VALUE, value2);
        
        jsonArray.add(item1);
        jsonArray.add(item2);
        json.put(Constants.Stats.ITEMS, jsonArray);
        return json;
    }

    private void combinePercentStat(Entry<String, JSONObject> currentStat, JSONObject combinedStatsJson)
            throws NumberFormatException {

        JSONObject stat = currentStat.getValue();

        String statValueA = stat.get(Constants.Stats.VALUE_A).toString();
        String statValueB = stat.get(Constants.Stats.VALUE_B).toString();

        JSONObject combined = (JSONObject) combinedStatsJson.get(currentStat.getKey());

        String combinedValueA = (String) combined.get(Constants.Stats.VALUE_A);
        String combinedValueB = (String) combined.get(Constants.Stats.VALUE_B);

        if (combinedValueA != null && combinedValueB != null) {

            Double valA = Double.parseDouble(combinedValueA);
            Double valB = Double.parseDouble(combinedValueB);

            Double oldValA = Double.parseDouble(statValueA);
            Double oldValB = Double.parseDouble(statValueB);

            Double cValA = (oldValA + valA) / 2;
            Double cValB = (oldValB + valB) / 2;

            combined.put(Constants.Stats.VALUE_A, cValA.toString());
            combined.put(Constants.Stats.VALUE_B, cValB.toString());

        }

        combinedStatsJson.put(currentStat.getKey(), combined);
    }

    private void combineFullMatchingStat(Entry<String, JSONObject> currentStat, JSONObject combinedStatsJson)
            throws NumberFormatException {

        JSONObject stat = currentStat.getValue();
        String statBoth = stat.get(Constants.Stats.VALUE_BOTH).toString();
        JSONObject combined = (JSONObject) combinedStatsJson.get(currentStat.getKey());
        String combinedBoth = (String) combined.get(Constants.Stats.VALUE_BOTH);

        if (combinedBoth != null) {
            Double both = Double.parseDouble(combinedBoth);
            Double oldBoth = Double.parseDouble(statBoth);
            Double combinedBothValue = oldBoth + both;
            combined.put(Constants.Stats.VALUE_BOTH, combinedBothValue.toString());
        }
        combinedStatsJson.put(currentStat.getKey(), combined);
    }

    private void combineCustomStat(Entry<String, JSONObject> currentStat, JSONObject combinedStatsJson)
            throws NumberFormatException {

        JSONObject stat = currentStat.getValue();
        String statValueA = stat.get(Constants.Stats.VALUE_A).toString();
        String statValueB = stat.get(Constants.Stats.VALUE_B).toString();

        JSONObject combined = (JSONObject) combinedStatsJson.get(currentStat.getKey());

        String combinedValueA = (String) combined.get(Constants.Stats.VALUE_A);
        String combinedValueB = (String) combined.get(Constants.Stats.VALUE_B);

        if (combinedValueA != null && combinedValueB != null) {
            Double valA = Double.parseDouble(combinedValueA);
            Double valB = Double.parseDouble(combinedValueB);
            Double oldValA = Double.parseDouble(statValueA);
            Double oldValB = Double.parseDouble(statValueB);
            Double cValA = oldValA + valA;
            Double cValB = oldValB + valB;

            combined.put(Constants.Stats.VALUE_A, cValA.toString());
            combined.put(Constants.Stats.VALUE_B, cValB.toString());
        }

        combinedStatsJson.put(currentStat.getKey(), combined);
    }

    private void combineFused(String fusedFilepath, String unlinkedA) throws IOException {

        Path outFile = Paths.get(fusedFilepath);

        try (FileChannel out = FileChannel.open(outFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            Path inFile1 = Paths.get(fusedFilepath + Constants.TEMP);
            try (FileChannel in = FileChannel.open(inFile1, StandardOpenOption.READ)) {
                for (long p = 0, l = in.size(); p < l;) {
                    p += in.transferTo(p, l - p, out);
                }
            }

            Path inFile2 = Paths.get(unlinkedA);
            try (FileChannel in = FileChannel.open(inFile2, StandardOpenOption.READ)) {
                for (long p = 0, l = in.size(); p < l;) {
                    p += in.transferTo(p, l - p, out);
                }
            }
        }

        LOG.info("Deleting " + fusedFilepath + Constants.TEMP);
        new File(fusedFilepath + Constants.TEMP).delete();
        LOG.info("Fused combined at " + fusedFilepath + Constants.TEMP + " complete.");
    }

    public static String getFormattedTime(long millis) {
        String time;
        if(millis < 1000){
            time = millis + "ms";
        } else {
            time = String.format("%02d min, %02d sec",
                    TimeUnit.MILLISECONDS.toMinutes(millis),
                    TimeUnit.MILLISECONDS.toSeconds(millis)
                    - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
            );
        }
        return time;
    }

    private void copyRemaining(String source, String target) throws IOException {
        Path sourcePath = Paths.get(source);
        Path targetPath = Paths.get(target);
        Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
    }
}
