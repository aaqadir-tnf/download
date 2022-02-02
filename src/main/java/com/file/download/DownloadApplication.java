package com.file.download;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.zeroturnaround.zip.ZipUtil;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

@SpringBootApplication
public class DownloadApplication {
    private static final Logger logger = LogManager.getLogger(DownloadApplication.class);

    static String destination = "/home/dartsapp/temp/";
    //static String destination = "D:\\ToBeDeleted\\";


    public static void main(String[] args) throws IOException {
        SpringApplication.run(DownloadApplication.class, args);

        //try (Reader reader = Files.newBufferedReader(Paths.get("D:\\demo_projects\\download\\src\\main\\resources\\inputcsv.csv"));
        try (Reader reader = Files.newBufferedReader(Paths.get("swwf_inputcsv.csv"));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            for (CSVRecord csvRecord : csvParser) {
                String name = csvRecord.get("name");
                String archive_location = csvRecord.get("archive_location");
                logger.info("Name: " + name);
                logger.info("Archive_location: " + archive_location);

                if (Files.isDirectory(Paths.get(archive_location))) {
                    Instant zipStart = Instant.now();
                    logger.info("----Directory Exist, start zipping---");

                    String zipFilePath = destination + name + ".zip";
                    //ZipUtil.pack(new File("D:\\ToBeDeleted\\swwf.cab81.0138_b313822f-0bdd-489b-b4f7-f01d7175f35e"), new File(destination+name+".zip"));
                    ZipUtil.pack(new File(archive_location), new File(zipFilePath));
                    Instant uploadStart = Instant.now();
                    logger.info("----start uploading---");
                    //uploadObject(new File(zipFilePath));
                    uploadObjectUsingCLI(new File(zipFilePath));

                    Instant uploadEnd = Instant.now();
                    logger.info("----uploading Done---");

                    Duration ziptimeElapsed = Duration.between(zipStart, uploadStart);
                    Duration uploadtimeElapsed = Duration.between(uploadStart, uploadEnd);

                    logger.info("Time taken to zip: "+ ziptimeElapsed.toMillis() +" milliseconds");
                    logger.info("Time taken to upload: "+ uploadtimeElapsed.toMillis() +" milliseconds");

                } else {
                    logger.info("***Not found, Hence ignoring this path****");
                }

            }
        }
    }

    private static File uploadObject(File file) {
        logger.info("----uploadObject method called---");
        Regions clientRegion = Regions.US_EAST_2;
        String bucketName = "coherent-commons-digital-assets-source";
        String nameOfFileToStore = file.getName();
        try {
            AmazonS3 s3client = AmazonS3ClientBuilder
                    .standard()
                    .withRegion(clientRegion)
                    .withCredentials(new DefaultAWSCredentialsProviderChain())
                    .build();
            logger.info("----AmazonS3ClientBuilder build---");
            s3client.putObject(new PutObjectRequest(bucketName, nameOfFileToStore, file)
                    .withCannedAcl(CannedAccessControlList.BucketOwnerFullControl));

        } catch (SdkClientException e) {
            logger.info("----SdkClientException---");
            e.printStackTrace();
        }
        return file;
    }

    private static void uploadObjectUsingCLI(File file) {
        logger.info("----uploadObjectUsingCLI method called---");
        Regions clientRegion = Regions.US_EAST_2;
        String bucketName = "coherent-commons-digital-assets-source";
        String nameOfFileToStore = file.getName();
        try {
            Instant start = Instant.now();
            // Get an object and print its contents.
            logger.info("---uploading an object with name :: "+nameOfFileToStore + " to bucket :: "+bucketName+"/"+nameOfFileToStore);
            ProcessBuilder processBuilder = new ProcessBuilder();
            //String command = "aws s3 cp "+ nameOfFileToStore +"s3://"+bucketName;
            String command = "aws s3 mv "+ nameOfFileToStore +" s3://"+bucketName;
            logger.info("---AWS cli:: "+command);
            processBuilder.command("bash", "-c", command);
            Process process = processBuilder.start();

            Instant end = Instant.now();
            Duration timeElapsed = Duration.between(start, end);
            logger.info("---Time taken uploadObjectUsingCLI: "+ timeElapsed.toMillis() +" milliseconds");

        } catch (SdkClientException | IOException e) {
            e.printStackTrace();
        }
    }
}

