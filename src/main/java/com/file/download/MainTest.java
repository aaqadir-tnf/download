package com.file.download;

import org.springframework.util.ObjectUtils;
import org.springframework.util.xml.SimpleNamespaceContext;

import java.io.IOException;

public class MainTest {
    private static final String VOLUME_FIELD = "V";
    private static final String FIRST_ISSUE_FIELD = "I";
    private static final String LAST_ISSUE_FIELD = "J";
    private static final String SUPPLEMENT_FIELD = "S";
    private static final String FIELD_SEPARATOR = "_";


    public static void main(String[] args) {
        String firstVolumeNumber = "10";
        String lastVolumeNumber = "12";

        String firstIssueNumber = "14";
        String lastIssueNumber = "15";

        String getSupplementNumber = "";

        StringBuilder str = new StringBuilder();

        str.append(VOLUME_FIELD);
        str.append(firstVolumeNumber);

        str.append("_");

        str.append(FIRST_ISSUE_FIELD);
        str.append(firstIssueNumber);

        str.append("_");

        str.append(LAST_ISSUE_FIELD);
        str.append(lastIssueNumber);

        str.append("_");

        str.append(SUPPLEMENT_FIELD);
        str.append(getSupplementNumber);

        System.out.println(str.toString());
    }
}
