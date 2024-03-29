package com.example.demo;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class EnvConfig {
    private static EnvConfig instance;
    private Properties properties;

    private EnvConfig() throws IOException {
        properties = new Properties();
        properties.load(new FileInputStream(".env"));
    }

    public static EnvConfig getInstance() throws IOException {
        if (instance == null) {
            instance = new EnvConfig();
        }
        return instance;
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }
}
