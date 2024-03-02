package jdbc;

import javax.sql.DataSource;

import lombok.Getter;
import lombok.Setter;

import java.io.*;
import java.sql.Connection;
import java.sql.ConnectionBuilder;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;


@Getter
@Setter
public class CustomDataSource implements DataSource {
    private static volatile CustomDataSource instance;
    private final String driver;
    private final String url;
    private final String name;
    private final String password;

    private static final String PROPERTIES_PATH = "src/main/resources/app.properties";
    private static final String PROPERTY_PASSWORD = "postgres.password";
    private static final String PROPERTY_USERNAME = "postgres.name";
    private static final String PROPERTY_URL = "postgres.url";
    private static final String PROPERTY_DRIVER = "postgres.driver";

    private final CustomConnector connector = new CustomConnector();
    private PrintWriter logWriter;
    private int loginTimeout = 0;
    private CustomDataSource(String driver, String url, String password, String name) {
        this.driver = driver;
        this.url = url;
        this.password = password;
        this.name = name;
    }

    private static Map<String, String> getProperties(String path, String... propName) {
        Map<String, String> mapProperties = null;
        try(InputStream propStream = new FileInputStream(path)) {
            Properties properties = new Properties();
            properties.load(CustomDataSource.class.getClassLoader().getResourceAsStream("app.properties"));
            mapProperties = new HashMap<>();
            for (String s : propName) {
                mapProperties.put(s, properties.getProperty(s));
            }
            System.out.println(mapProperties.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mapProperties;
    }

    public static CustomDataSource getInstance() {
        if(instance == null) {
            synchronized (CustomDataSource.class) {
                if(instance == null) {
                    Map<String, String> properties = getProperties(PROPERTIES_PATH, PROPERTY_DRIVER, PROPERTY_URL,
                            PROPERTY_USERNAME, PROPERTY_PASSWORD);
                    instance = new CustomDataSource(properties.get(PROPERTY_DRIVER) ,properties.get(PROPERTY_URL),
                            properties.get(PROPERTY_PASSWORD), properties.get(PROPERTY_USERNAME));
                }
            }
        }
        return instance;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connector.getConnection(this.url, this.name, this.password);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return connector.getConnection(this.url, username, password);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return this.logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.logWriter = new PrintWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        loginTimeout = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ConnectionBuilder createConnectionBuilder() throws SQLException {
        return DataSource.super.createConnectionBuilder();
    }
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if(iface.isAssignableFrom(this.getClass())) {
            return iface.cast(this);
        }
        else {
            throw new SQLException("Cannot unwrap to" + iface.getName());
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(this.getClass());
    }
}