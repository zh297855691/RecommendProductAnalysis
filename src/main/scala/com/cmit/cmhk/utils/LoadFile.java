package com.cmit.cmhk.utils;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * 加载配置文件类
 */
public class LoadFile {

    private static Logger log = Logger.getLogger(LoadFile.class.getName());

    public static Properties loadConfig(String configFile) {

        Properties prop = new Properties();
        try{
            prop.load(LoadFile.class.getClassLoader().getResourceAsStream(configFile));
//            prop.load(new FileInputStream(new File(configFile)));
        } catch (FileNotFoundException e) {
            log.error("找不到配置文件: " + configFile, e);
            throw new RuntimeException("找不到配置文件: " + configFile);
        } catch (IOException e) {
            log.error("加载配置文件: " + configFile + "失败！", e);
            throw new RuntimeException("加载配置文件: " + configFile + "失败！");
        } catch (Exception e) {
            log.error("获取配置文件: " + configFile, e);
            throw new RuntimeException("获取配置文件: " + configFile + "失败！");
        }
        return prop;

    }

}
