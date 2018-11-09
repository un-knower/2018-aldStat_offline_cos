package aldwxconfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 获取配置中的信息
 * 这些信息可以包含 hdfs 地址, mysql 连接信息等
 */
public class ConfigurationUtil {

    /**
     * 根据 key 获取 properties 文件中的 value
     * @param key properties 文件中等号左边的键
     * @return 返回 properties 文件中等号右边的值
     */
    public static String getProperty(String key) {
        Properties properties = new Properties();
        InputStream in = ConfigurationUtil.class.getClassLoader().getResourceAsStream(getEnvProperty("env.conf"));
        try {
            properties.load(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (String) properties.get(key);
    }

    /**
     * 根据 key 获取 env.properties 中的 value
     * @param  key env.properties 文件中等号左边的 key
     * @return 返回 env.properties 文件中等号右边的值
     */
    public static String getEnvProperty(String key) {
        Properties properties = new Properties();
        InputStream in = ConfigurationUtil.class.getClassLoader().getResourceAsStream("env.properties");
        try {
            properties.load(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (String) properties.get(key);
    }

    /**
     * 获取整数类型的配置项
     *
     * @param key
     * @return value
     */
    public static Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取布尔类型的配置项
     *
     * @param key
     * @return value
     */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 获取Long类型的配置项
     *
     * @param key
     * @return
     */
    public static Long getLong(String key) {
        String value = getProperty(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }
}
