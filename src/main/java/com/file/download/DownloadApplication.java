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
import org.zeroturnaround.zip.ZipUtil;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class DownloadApplication {
    private static final Logger logger = LogManager.getLogger(DownloadApplication.class);

    static String destination = "/home/dartsapp/temp/";
    //static String destination = "/home/dartsapp/temp/SWWF/";

    public static void main(String[] args) {
        SpringApplication.run(DownloadApplication.class, args);

        try (Reader reader = Files.newBufferedReader(Paths.get("swwf_inputcsv.csv"));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            ProcessBuilder processBuilder = new ProcessBuilder();
            for (CSVRecord csvRecord : csvParser) {
                String name = csvRecord.get("name");
                String archive_location = csvRecord.get("archive_location");
                logger.info("Name: " + name);
                logger.info("Archive_location: " + archive_location);

                if (Files.isDirectory(Paths.get(archive_location))) {
                    Instant zipStart = Instant.now();
                    logger.info("----Directory Exist, start zipping---");

                    String zipFilePath = destination + name + ".zip";
                    ZipUtil.pack(new File(archive_location), new File(zipFilePath));
                    Instant uploadStart = Instant.now();
                    logger.info("----start uploading---");
                    uploadObjectUsingCLI(new File(zipFilePath), processBuilder);
                    appendOutputFile(name);

                    //Instant uploadEnd = Instant.now();
                    logger.info("Time taken to zip: "+ Duration.between(zipStart, uploadStart) +" milliseconds");
                    //logger.info("Time taken to upload: "+ Duration.between(uploadStart, uploadEnd) +" milliseconds");
                } else {
                    logger.info("***Not found, Hence ignoring this path****");
                }
            }
        } catch (IOException ex){
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

    //upload using AWS CLI, its is comparatively faster
    private static void uploadObjectUsingCLI(File file, ProcessBuilder processBuilder) {
        logger.info("----uploadObjectUsingCLI start---");
        String bucketName = "coherent-commons-digital-assets-source";
        String bucketFullPath = bucketName+"/SEFI/"; //folder for SWWF
        String nameOfFileToStore = file.getName();
        try {
            // Get an object and print its contents.
            StringBuilder com = new StringBuilder();
            com.append("aws s3 mv ");
            com.append(nameOfFileToStore);
            com.append(" s3://");
            com.append(bucketFullPath);

            logger.info("---AWS cli:: "+com);
            processBuilder.command("bash", "-c", String.valueOf(com));
            Process proStart = processBuilder.start();

            stopProcessOnCompletion(proStart);

            logger.info("---uploadObjectUsingCLI completed");
        } catch (SdkClientException | IOException e) {
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

