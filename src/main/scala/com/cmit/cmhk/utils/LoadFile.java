package com.cmit.cmhk.utils;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * ���������ļ���
 */
public class LoadFile {

    private static Logger log = Logger.getLogger(LoadFile.class.getName());

    public static Properties loadConfig(String configFile) {

        Properties prop = new Properties();
        try{
            prop.load(LoadFile.class.getClassLoader().getResourceAsStream(configFile));
//            prop.load(new FileInputStream(new File(configFile)));
        } catch (FileNotFoundException e) {
            log.error("�Ҳ��������ļ�: " + configFile, e);
            throw new RuntimeException("�Ҳ��������ļ�: " + configFile);
        } catch (IOException e) {
            log.error("���������ļ�: " + configFile + "ʧ�ܣ�", e);
            throw new RuntimeException("���������ļ�: " + configFile + "ʧ�ܣ�");
        } catch (Exception e) {
            log.error("��ȡ�����ļ�: " + configFile, e);
            throw new RuntimeException("��ȡ�����ļ�: " + configFile + "ʧ�ܣ�");
        }
        return prop;

    }

}
