package com.jpmc.midascore;

import org.apache.commons.io.IOUtils;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Component
public class FileLoader {
    public String[] loadStrings(String path) {
        try {
            InputStream inputStream = this.getClass().getResourceAsStream(path);
            String fileText = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            return fileText.split(System.lineSeparator());
        } catch (Exception e) {
            return null;
        }
    }
}
