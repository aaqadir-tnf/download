package com.file.download;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.opencsv.CSVWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

@SpringBootApplication
public class DownloadApplication {
    private static final Logger logger = LogManager.getLogger(DownloadApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(DownloadApplication.class, args);

        File inputFile = new File("TRES_I_42_15_J.zip");

        Instant uploadStart = Instant.now();
        uploadObject(inputFile);
        Instant uploadEnd = Instant.now();

        appendOutputFile(inputFile.getName());
        logger.info("Time taken to zip: " + Duration.between(uploadStart, uploadEnd) + " milliseconds");

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

    //upload using AWS SDK, its is comparatively slower than AWS CLI
    private static File uploadObject(File file) {
        logger.info("----uploadObject method called---");
        Regions clientRegion = Regions.EU_WEST_1;
        String bucketName = "s3-euw1-ap-pe-df-pch-journals-source-d";
        String nameOfFileToStore = file.getName();
        try {
            logger.info("----AmazonS3ClientBuilder building---");
            AmazonS3 s3client = AmazonS3ClientBuilder
                    .standard()
                    .withRegion(clientRegion)
                    .withCredentials(new DefaultAWSCredentialsProviderChain())
                    .build();

            logger.info("----AmazonS3ClientBuilder build---");
            TransferManager transferManager = TransferManagerBuilder.standard()
                    .withS3Client(s3client)
                    .withMinimumUploadPartSize((long)(100 * 1024 * 1025))
                    .withMultipartUploadThreshold((long)(50 * 1024 * 1025))
                    .build();

            Upload upload = transferManager.upload(bucketName, nameOfFileToStore, file);
            upload.addProgressListener((ProgressListener) e -> logger.info("---Transferring file - " + e.getBytesTransferred()));
            upload.waitForCompletion();
            transferManager.shutdownNow();

        } catch (SdkClientException | InterruptedException e) {
            logger.info("----SdkClientException---");
            e.printStackTrace();
        }
        return file;
    }
}

