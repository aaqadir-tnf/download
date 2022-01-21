package com.file.download;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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

@SpringBootApplication
public class DownloadApplication {

    static String destination = "/home/dartsapp/temp/DARTS-1456/";
    //static String destination = "D:\\ToBeDeleted\\";


    public static void main(String[] args) throws IOException {
        SpringApplication.run(DownloadApplication.class, args);

        //try (Reader reader = Files.newBufferedReader(Paths.get("D:\\demo_projects\\download\\src\\main\\resources\\inputcsv.csv"));
        try (Reader reader = Files.newBufferedReader(Paths.get("inputcsv.csv"));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            for (CSVRecord csvRecord : csvParser) {
                String name = csvRecord.get("name");
                String archive_location = csvRecord.get("archive_location");
                System.out.println("Name: " + name);
                System.out.println("Archive_location: " + archive_location);

                if (Files.isDirectory(Paths.get(archive_location))) {
                    System.out.println("----Directory Exist---");

                    String zipFilelName = destination + name + ".zip";
                    //ZipUtil.pack(new File("D:\\ToBeDeleted\\swwf.cab81.0138_b313822f-0bdd-489b-b4f7-f01d7175f35e"), new File(destination+name+".zip"));
                    ZipUtil.pack(new File(archive_location), new File(zipFilelName));
                    uploadObject(zipFilelName);
                } else {
                    System.out.println("***Not found, Hence ignoring this path****");
                }

            }

        }
    }

    private static void uploadObject(String zipFilelName) {

        Regions clientRegion = Regions.DEFAULT_REGION;
        String bucketName = "*** Bucket name ***";
        String stringObjKeyName = "*** String object key name ***";
        String fileObjKeyName = "*** File object key name ***";
        String fileName = "*** Path to file to upload ***";

        try {
            AmazonS3 s3client = AmazonS3ClientBuilder
                    .standard()
                    .withRegion(Regions.fromName(""))
                    .withCredentials(new DefaultAWSCredentialsProviderChain())
                    .build();
            //This code expects that you have AWS credentials set up per:
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .build();

            // Upload a text string as a new object.
            s3Client.putObject(bucketName, stringObjKeyName, "Uploaded String Object");

            // Upload a file as a new object with ContentType and title specified.
            PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(fileName));
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("plain/text");
            metadata.addUserMetadata("title", "someTitle");
            request.setMetadata(metadata);
            s3Client.putObject(request);
        } catch (SdkClientException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        }
        // Amazon S3 couldn't be contacted for a response, or the client
        // couldn't parse the response from Amazon S3.
    }
}

