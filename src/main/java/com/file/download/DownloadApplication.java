package com.file.download;

import com.amazonaws.SdkClientException;
import com.opencsv.CSVWriter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class DownloadApplication {
    private static final Logger logger = LogManager.getLogger(DownloadApplication.class);

    static String destination = "/home/dartsapp/temp/";
    static String inputFile = "dove_issues.csv";


    public static void main(String[] args) {
        SpringApplication.run(DownloadApplication.class, args);

        try (Reader reader = Files.newBufferedReader(Paths.get(inputFile));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            for (CSVRecord csvRecord : csvParser) {
                String name = csvRecord.get("name");
                String archiveLocation = csvRecord.get("archive_location");
                logger.info("Name: " + name);
                logger.info("ArchiveLocation: " + archiveLocation);

                if (Files.isDirectory(Paths.get(archiveLocation))) {
                    logger.info("----Directory Exist, GetNames---");
                    File directoryPath = new File(archiveLocation);
                    String[] contents = directoryPath.list();

                    ArrayList<String> arrayList = new ArrayList<>(Arrays.asList(contents));
                    logger.info("----content found: " + arrayList.size());
                    logger.info("----appendOutputFile start---");
                    //all issues output in one file
                    //try (CSVWriter csvWriter = new CSVWriter(new FileWriter("ListOfArticles" + ".csv", true))) {
                        //each issues output will be in their individual file
                      try (CSVWriter csvWriter = new CSVWriter(new FileWriter(name + ".csv", true))) {
                        csvWriter.writeNext(new String[]{name + ": " + arrayList.size() + " items found\n"});
                        csvWriter.writeNext(new String[]{Arrays.toString(contents) + "\n"});
                        logger.info("----csvWriter end---");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    logger.info("***Not found, Hence ignoring this path****");
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void appendOutputFile(String name) {
        logger.info("----appendOutputFile start---");
        try (CSVWriter csvWriter = new CSVWriter(new FileWriter("output.csv", true))) {
            csvWriter.writeNext(new String[]{name});
            logger.info("----csvWriter end---");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void stopProcessOnCompletion(Process process) {
        logger.info("---stopProcessOnCompletion");
        try {
            process.waitFor(1, TimeUnit.MINUTES);
            logger.info("---waitFor 1 minutes");
            process.destroy();
            logger.info("---destroy called");
            process.waitFor();
            logger.info("---process destroyed");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}

