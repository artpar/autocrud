package io.artpar.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.artpar.curd.SchemaController;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Environment;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by parth on 30/4/16.
 */
public class TestService extends Application<TestService.TestConfiguration> {
    @Override
    public void run(TestConfiguration configuration, Environment environment) throws Exception {
        HikariConfig haikiConfig = new HikariConfig();
        haikiConfig.setJdbcUrl("jdbc:mysql://localhost:3306/inf?zeroDateTimeBehavior=convertToNull");
        haikiConfig.setUsername("root");
        haikiConfig.setPassword("parth123");

        environment.jersey().register(InsertDummyUserFilter.class);
        SchemaController res = new SchemaController("/", "crud", new HikariDataSource(haikiConfig), new ObjectMapper());
        environment.jersey().getResourceConfig().registerResources(res.getRootResource().build());
        environment.jersey().getResourceConfig().getEndpointsInfo();

    }
    public static void main(String[] args) throws Exception {
        new TestService().run(args);
    }

    public static class TestConfiguration extends Configuration {

    }

}
