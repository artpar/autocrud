package io.artpar.curd;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.jersey.process.Inflector;
import org.glassfish.jersey.server.model.Resource;
import org.glassfish.jersey.server.model.ResourceMethod;

import javax.sql.DataSource;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MediaType;
import java.sql.*;
import java.util.*;

/**
 * Created by parth on 30/4/16.
 */
public class SchemaController extends AbstractController {

    @Override
    protected Integer getTotalCount() throws SQLException {
        return tableNames.size();
    }

    public SchemaController(String root, DataSource dataSource, ObjectMapper objectMapper) throws SQLException, NoSuchMethodException {
        super(root, dataSource, objectMapper);
    }


    protected void init() throws SQLException, NoSuchMethodException {
        String catalog = null;
        String schemaPattern = null;
        String tableNamePattern = null;
        String[] types = null;

        Connection connection = this.dataSource.getConnection();
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        connection.close();
        ResultSet result = databaseMetaData.getTables(catalog, schemaPattern, tableNamePattern, types);
        List<String> tableNameList = getSingleColumnFromResultSet(result, 3);
        for (String s : tableNameList) {
            tableNames.put(s, new TableData());
        }

        result.close();

        addMethod(rootResource, "GET", new Inflector<ContainerRequestContext, Object>() {
            @Override
            public Set<String> apply(ContainerRequestContext containerRequestContext) {
                return tableNames.keySet();
            }
        });

        for (final String tableName : tableNames.keySet()) {
            Resource.Builder resourceBuilder = Resource.builder();
            resourceBuilder = getAddMethods(tableName, resourceBuilder);
            rootResource.addChildResource(resourceBuilder.build());
        }
    }

    private void addMethod(Resource.Builder resourceBuilder, String method, Inflector<ContainerRequestContext, Object> handler) {
        ResourceMethod.Builder methodBuilder = resourceBuilder.addMethod(method);
        methodBuilder.produces(MediaType.APPLICATION_JSON).handledBy(handler);
    }

    private Resource.Builder getAddMethods(final String tableName, Resource.Builder resourceBuilder) throws SQLException, NoSuchMethodException {
        resourceBuilder.path(tableName);
        info("    GET    /%s", tableName);

        TableController tableController = new TableController(tableName, dataSource, objectMapper);
        this.rootResource.addChildResource(tableController.getRootResource().build());

        Resource.Builder getIndividual = Resource.builder();
        getIndividual.path(tableName + "/{id}");


        addMethod(getIndividual, "GET",new Inflector<ContainerRequestContext, Object>() {
            @Override
            public Object apply(ContainerRequestContext containerRequestContext) {
                return null;
            }
        } );

        resourceBuilder.addChildResource(getIndividual.build());


        return resourceBuilder;
    }

}





