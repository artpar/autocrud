package io.artpar.curd;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.jersey.process.Inflector;
import org.glassfish.jersey.server.model.Resource;
import org.glassfish.jersey.server.model.ResourceMethod;

import javax.annotation.security.RolesAllowed;
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
    public boolean isPermissionOk(boolean b, UserInterface userInterface, Map obj) {
        return true;
    }

    @Override
    protected Integer getTotalCount() throws SQLException {
        return tableNames.size();
    }

    public SchemaController(  String context, String root, DataSource dataSource, ObjectMapper objectMapper) throws SQLException, NoSuchMethodException {
        super(context, root, dataSource, objectMapper);
        init();
    }


    protected void init() throws SQLException, NoSuchMethodException {
        String catalog = null;
        String schemaPattern = null;
        String tableNamePattern = null;
        String[] types = null;

        Connection connection = this.dataSource.getConnection();
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        ResultSet result = databaseMetaData.getTables(catalog, schemaPattern, tableNamePattern, types);
        List<String> tableNameList = getSingleColumnFromResultSet(result, 3);
        for (String s : tableNameList) {
            tableNames.put(s, new TableData());
        }
        connection.close();

        result.close();

        addMethod(rootResource, "GET", new Inflector<ContainerRequestContext, Object>() {
            @Override
            @RolesAllowed("ROLE_USER")
            public Set<String> apply(ContainerRequestContext containerRequestContext) {
                return tableNames.keySet();
            }
        });

        boolean worldOk = false;
        List<String> finalList = new LinkedList<>();
        for (final String tableName : tableNames.keySet()) {
            if (tableName.equalsIgnoreCase("world")) {
                worldOk = true;
            }
             finalList.add(tableName);
        }
        if (!worldOk) {
            addTableResource("world");
        }
        for (String s : finalList) {
            addTableResource(s);
        }

    }

    private void addMethod(Resource.Builder resourceBuilder, String method, Inflector<ContainerRequestContext, Object> handler) {
        ResourceMethod.Builder methodBuilder = resourceBuilder.addMethod(method);
        methodBuilder.produces(MediaType.APPLICATION_JSON).handledBy(handler);
    }

    private void addTableResource(final String tableName) throws SQLException, NoSuchMethodException {
//        resourceBuilder.path(tableName);

        AbstractTableController abstractTableController = new TableController(tableName, this.context + "/", tableName, dataSource, objectMapper);
        this.rootResource.addChildResource(abstractTableController.getRootResource().build());
        MineController tableController1 = new MineController(tableName, this.context + "/", tableName + "/mine", dataSource, objectMapper);
        this.rootResource.addChildResource(tableController1.getRootResource().build());

    }

}





