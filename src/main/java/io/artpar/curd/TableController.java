package io.artpar.curd;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jersey.jackson.JsonProcessingExceptionMapper;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.model.Resource;

import javax.annotation.security.RolesAllowed;
import javax.sql.DataSource;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * Created by parth on 30/4/16.
 */
public class TableController extends AbstractTableController {


    public TableController(String tableName, String context, String contextPath, DataSource dataSource, ObjectMapper objectMapper) throws SQLException, NoSuchMethodException {
        super(tableName, context, contextPath, dataSource, objectMapper);

        final Method router = TableController.class.getMethod("router", ContainerRequestContext.class);

        debug("Added GET " + this.context);
        this.rootResource.addMethod("GET").produces(MediaType.APPLICATION_JSON_TYPE).handledBy(this, router);

        debug("Added POST " + this.context);
        this.rootResource.addMethod("POST").produces(MediaType.APPLICATION_JSON_TYPE).handledBy(this, router);


        debug("Added PUT " + this.context);
        this.rootResource.addMethod("PUT").produces(MediaType.APPLICATION_JSON_TYPE).handledBy(this, router);

        debug("Added DELETE " + this.context);
        this.rootResource.addMethod("DELETE").produces(MediaType.APPLICATION_JSON_TYPE).handledBy(this, router);


        Resource.Builder re = this.rootResource.addChildResource("mine");
        re.addMethod("GET").produces(MediaType.APPLICATION_JSON_TYPE).handledBy(this, router);
//        this.rootResource.addChildResource(re.build());
        debug("Added GET " + this.context + "/mine");
    }

    @Override
    @RolesAllowed("ROLE_USER")
    public Object router(ContainerRequestContext containerRequestContext) throws IOException, SQLException {
        final String path = containerRequestContext.getUriInfo().getPath();
        final int lastSlash = path.lastIndexOf("/");
        String ourName = path.substring(lastSlash + 1);
        final MyRequest myRequest = new MyRequest(containerRequestContext);
        Object re = permissionCheck(myRequest);
        if (re != null) {
            return re;
        }


        String relative = path.substring(path.indexOf(this.root) + this.root.length()) + "/";

        switch (containerRequestContext.getMethod().toLowerCase() + " " + relative) {
            case "get /":
                return this.list(myRequest);
            case "post /":
                return this.newItem(myRequest);
            case "put /":
                return this.updateItem(myRequest);
            case "delete /":
                return this.deleteItem(myRequest);
            case "options /":
                return this.listMyItem(myRequest);
        }
        return Response.status(Response.Status.NOT_IMPLEMENTED);
    }

    private Object listMyItem(MyRequest myRequest) {
        MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
        params.putSingle("user_id", String.valueOf(myRequest.getUser().getId()));
        return getResult(params, myRequest.getUser());
    }

    public Object deleteItem(MyRequest containerRequestContext) throws IOException, SQLException {
        Map values = containerRequestContext.getBodyValueMap();
        debug("Request object: %s", values);
        List<String> allColumns = tableData.getColumnList();
        List<String> colsToInsert = new LinkedList<>();
        List<Object> valueList = new LinkedList<>();
        for (Object col : values.keySet()) {
            String colName = (String) col;
            if (allColumns.contains(colName)) {
                colsToInsert.add(colName);
                valueList.add(values.get(colName));
            }
        }


        final Object reference_id = containerRequestContext.getOriginalValue("reference_id");
        if (reference_id == null || reference_id.toString().length() < 1) {
            return Response.status(Response.Status.BAD_REQUEST);
        }


        String referenceId = String.valueOf(reference_id);
        valueList.add(referenceId);

        String sql = "delete from " + tableName + " ";
        final int secondLast = colsToInsert.size() - 1;
        for (int i = 0; i < colsToInsert.size(); i++) {
            String s = colsToInsert.get(i);
            sql = sql + s + "=?";
            if (i < secondLast) {
                sql = sql + ", ";
            }
        }

        sql = sql + " where reference_id=?";

        Connection connection = dataSource.getConnection();
        logger.debug("execute update for " + tableName + "\n" + sql);
        PreparedStatement ps = connection.prepareStatement(sql);
        for (int i = 1; i <= valueList.size(); i++) {
            Object s = valueList.get(i - 1);
            ps.setObject(i, s);
        }
        ps.execute();
        values.put("reference_id", referenceId);
        ps.close();
        connection.close();
        return values;
    }

    public Object updateItem(MyRequest containerRequestContext) throws IOException, SQLException {
        Map values = containerRequestContext.getBodyValueMap();
        debug("Request object: %s", values);
        List<String> allColumns = tableData.getColumnList();
        List<String> colsToInsert = new LinkedList<>();
        List<Object> valueList = new LinkedList<>();
        for (Object col : values.keySet()) {
            String colName = (String) col;
            if (allColumns.contains(colName)) {
                colsToInsert.add(colName);
                valueList.add(values.get(colName));
            }
        }


        final Object reference_id = containerRequestContext.getOriginalValue("reference_id");
        if (reference_id == null || reference_id.toString().length() < 1) {
            return Response.status(Response.Status.BAD_REQUEST);
        }


        String referenceId = String.valueOf(reference_id);
        valueList.add(referenceId);

        String sql = "update " + tableName + " set ";
        final int secondLast = colsToInsert.size() - 1;
        for (int i = 0; i < colsToInsert.size(); i++) {
            String s = colsToInsert.get(i);
            sql = sql + s + "=?";
            if (i < secondLast) {
                sql = sql + ", ";
            }
        }

        sql = sql + " where reference_id=?";

        Connection connection = dataSource.getConnection();
        logger.debug("execute update for " + tableName + "\n" + sql);
        PreparedStatement ps = connection.prepareStatement(sql);
        for (int i = 1; i <= valueList.size(); i++) {
            Object s = valueList.get(i - 1);
            ps.setObject(i, s);
        }
        ps.execute();
        values.put("reference_id", referenceId);
        ps.close();
        connection.close();
        return values;
    }

    public Object newItem(MyRequest containerRequestContext) throws IOException, SQLException {
        Map values = containerRequestContext.getBodyValueMap();
        UserInterface user = containerRequestContext.getUser();
        debug("Request object: %s", values);
        List<String> allColumns = tableData.getColumnList();
        List<String> colsToInsert = new LinkedList<>();
        List<Object> valueList = new LinkedList<>();
        String referenceId = UUID.randomUUID().toString();
        values.put("user_id", user.getId());
        values.put("usergroup_id", user.getUserGroupId().get(0));
        values.put("reference_id", referenceId);
        for (Object col : values.keySet()) {
            String colName = (String) col;
            if (allColumns.contains(colName)) {
                colsToInsert.add(colName);
                valueList.add(values.get(colName));
            }
        }

        String sql = "insert into " + tableName + "(" + String.join(",", colsToInsert) + ") values (" + String.join(",", new String(new char[colsToInsert.size()]).replace("\0", "?").split("")) + ")";

        Connection connection = dataSource.getConnection();
        PreparedStatement ps = connection.prepareStatement(sql);
        for (int i = 1; i <= valueList.size(); i++) {
            Object s = valueList.get(i - 1);
            ps.setObject(i, s);
        }
        ps.execute();
        ps.close();
        connection.close();
        return values;
    }

    public Object list(MyRequest containerRequestContext) {
        MultivaluedMap<String, String> queryParams = containerRequestContext.getQueryParameters();
        return getResult(queryParams, containerRequestContext.getUser());
    }


}
