package gr.athena.innovation.fagi.merger;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;

/**
 *
 * @author nkarag
 */
public class MergerInstance {

    private static final org.apache.logging.log4j.Logger LOG = LogManager.getLogger(MergerInstance.class);

    public void run(String configPath) throws WrongInputException, MergeOperationException{

        LOG.info("Merge process started.");
        long start = System.currentTimeMillis();

        ConfigParser parser = new ConfigParser();
        Configuration config = parser.parse(configPath);

        LOG.info("Configuration: " + config.toString());
        int partitions = config.getPartitions();
        String inputDir = config.getInputDir();
        String outputDir = config.getOutputDir();
        EnumFusionMode fusionMode = config.getFusionMode();

        mergeDataset(partitions, inputDir, outputDir, Constants.FUSED);
        mergeDataset(partitions, inputDir, outputDir, Constants.REMAINING);
        mergeDataset(partitions, inputDir, outputDir, Constants.LINKS);
        mergeDataset(partitions, inputDir, outputDir, Constants.AMBIGUOUS);
        //mergeDataset(partitions, inputDir, outputDir, Constants.STATS);

        long end = System.currentTimeMillis();

        LOG.info("Merge process complete.");
        String time = getFormattedTime(end - start);
        LOG.info("Time passed: " + time);
    }

    private void mergeDataset(int partitions, String inputDir, String outputDir, String datasetId) throws WrongInputException, MergeOperationException {
        
        File inputDirectory = new File(inputDir);
        File[] dirs = inputDirectory.listFiles();
        File[] partitionDirs = Arrays.stream(dirs)
                .filter(f -> f.getName().contains(Constants.PARTITION)).toArray(File[]::new);
        
        if(partitionDirs.length != partitions){
            LOG.error("Partitions found do not match configuration.");
            throw new WrongInputException("Partitions found do not match configuration.");
        }
        
        LOG.info(Arrays.asList(partitionDirs));
        
        List<String> leftPartitions = new ArrayList<>();
        for (File partitionDir : partitionDirs) {
            File[] files = partitionDir.listFiles();
            for (File file : files) {
                if (file.getName().contains(datasetId)) {
                    leftPartitions.add(file.getAbsolutePath());
                }
            }
        }
        
        try {
            String outputFilepath = outputDir + datasetId;
            merge(leftPartitions, outputFilepath);
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
        LOG.info("Done.");
    }

    public static String getFormattedTime(long millis) {
        String time = String.format("%02d min, %02d sec",
                TimeUnit.MILLISECONDS.toMinutes(millis),
                TimeUnit.MILLISECONDS.toSeconds(millis)
                - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
        );
        return time;
    }
}