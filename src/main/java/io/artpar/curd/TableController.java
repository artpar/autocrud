package io.artpar.curd;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.model.Resource;

import javax.sql.DataSource;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
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
public class TableController extends AbstractController {


    private Map<String, List<String>> AutoColumns;
    private static final List<String> UserTables = Arrays.asList("user", "user_usergroup", "usergroup");

    private String tableName;
    private TableData tableData;
    private static final Random rng = new Random(new Date().getTime());
    private List<String > ColumnsWeDontUpdate = Arrays.asList("id", "reference_id", "created_at", "updated_at");


    public TableController(String context, String tableName, DataSource dataSource, ObjectMapper objectMapper) throws SQLException, NoSuchMethodException {
        super(context, tableName, dataSource, objectMapper);

        final Method router = TableController.class.getMethod("router", ContainerRequestContext.class);

        info("Added GET " + this.context);
        this.rootResource.addMethod("GET").produces(MediaType.APPLICATION_JSON_TYPE).handledBy(this, router);

        info("Added POST " + this.context);
        this.rootResource.addMethod("POST").produces(MediaType.APPLICATION_JSON_TYPE).handledBy(this, router);


        info("Added PUT " + this.context + "/([a-z0-9\\-]+)");
        this.rootResource.addMethod("PUT").produces(MediaType.APPLICATION_JSON_TYPE).handledBy(this, router);

    }

    public void initColumnData() {
        AutoColumns = new HashMap<>();
        AutoColumns.put("id", Arrays.asList("alter table " + tableName + " add column `id` int(11) unsigned  primary key auto_increment"));
        AutoColumns.put("created_at", Arrays.asList("alter table " + tableName + " add column created_at timestamp default CURRENT_TIMESTAMP"));
        AutoColumns.put("updated_at", Arrays.asList("alter table " + tableName + " add column updated_at timestamp null default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP"));
        AutoColumns.put("reference_id", Arrays.asList("alter table " + tableName + " add column reference_id varchar(50)",
                "update " + tableName + " set reference_id = uuid() where reference_id = null ",
                "alter table " + tableName + " add constraint "+tableName+"_reference_id_uniq_"+rng.nextInt(10000 - 1) +" unique(reference_id)"));
        AutoColumns.put("permission", Arrays.asList("alter table " + tableName + " add column permission int(4) not null default 755"));
        AutoColumns.put("user_id", Arrays.asList("alter table " + tableName + " add column user_id int(11)"
                , "update " +  tableName + " set user_id = (select min(id) from user)"
                , "alter table " + tableName + " add constraint " + tableName + "_user_id_" + rng.nextInt(10000-1) + " foreign key (user_id) references user(id) "));
        AutoColumns.put("usergroup_id", Arrays.asList("alter table " + tableName + " add column usergroup_id int(11)"
                , "update " +  tableName + " set usergroup_id = (select min(id) from usergroup)"
                , "alter table " + tableName + " add constraint " + tableName + "_usergroup_id_" + rng.nextInt(10000-1) + " foreign key (usergroup_id) references usergroup(id) "));

    }


    class MyRequest {
        private Map<String, Object> originalValue = new HashMap<>();
        private ContainerRequestContext containerRequestContext;

        public MyRequest(ContainerRequestContext containerRequestContext) {
            this.containerRequestContext = containerRequestContext;
        }
        public Map getBodyValueMap() throws IOException {
            InputStream is = containerRequestContext.getEntityStream();
            final Map map = objectMapper.readValue(is, Map.class);
            originalValue.putAll(map);
            List<String> removeKey = new LinkedList<>();
            for (Object o : map.keySet()) {
                String key = (String) o;
                if(ColumnsWeDontUpdate.contains(key)) {
                    removeKey.add(key);
                }
            }
            for (String s : removeKey) {
                map.remove(s);
            }
            return map;
        }

        public UriInfo getUriInfo() {
            return containerRequestContext.getUriInfo();
        }

        public String getMethod() {
            return containerRequestContext.getMethod();
        }

        public URI getRequestUri() {
            return ((ContainerRequest)containerRequestContext).getRequestUri();
        }

        public Object getOriginalValue(String key) {
            return originalValue.get(key);
        }
    }

    public Object router(ContainerRequestContext containerRequestContext) throws IOException, SQLException {
        final String path = containerRequestContext.getUriInfo().getPath();
        final int lastSlash = path.lastIndexOf("/");
        String ourName = path.substring(lastSlash + 1);
        switch (containerRequestContext.getMethod().toLowerCase()) {
            case "get":
                return this.list(new MyRequest(containerRequestContext));
            case "post":
                return this.newItem(new MyRequest(containerRequestContext));
            case "put" :
                return this.updateItem(new MyRequest(containerRequestContext));
        }
        return Response.status(Response.Status.NOT_IMPLEMENTED);
    }

    public Object updateItem(MyRequest containerRequestContext) throws IOException, SQLException {
        Map values = containerRequestContext.getBodyValueMap();
        info("Request object: %s", values);
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
        logger.info("execute update for " + tableName + "\n" + sql);
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
        info("Request object: %s", values);
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


        String referenceId = UUID.randomUUID().toString();
        colsToInsert.add("reference_id");
        valueList.add(referenceId);

        String sql = "insert into " + tableName + "(" + String.join(",", colsToInsert) + ") values (" + String.join(",", new String(new char[colsToInsert.size()]).replace("\0", "?").split("")) + ")";

        Connection connection = dataSource.getConnection();
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

    public Object list(MyRequest containerRequestContext) {
        String query = containerRequestContext.getRequestUri().getQuery();

        try {

            List<String> columnNames = tableData.getColumnList();
            List<String> finalList = new LinkedList<>();
            Hashtable<String, String[]> queryParams = parseQueryString(query);
            String[] columnFilterList = queryParams.get("column");
            if (columnFilterList != null && columnFilterList[0].length() > 0) {
                String[] columnInRequest = queryParams.get("column");
                List<String> names = Arrays.asList(columnInRequest[0].split(","));
                for (String name : columnNames) {
                    if (names.contains(name)) {
                        finalList.add(name);
                    }
                }
                columnNames = finalList;
            }

            String[] where = queryParams.get("where");
            List<String> whereColumns = new LinkedList<>();
            List<Object> whereValues = new LinkedList<>();
            if (where != null && where[0].length() > 3) {
                String[] whereList = where[0].split(",");
                for (String s : whereList) {
                    String[] part = s.split(":", 2);
                    if (part.length == 2 && tableData.getColumnList().contains(part[0])) {
                        whereColumns.add(part[0]);
                        whereValues.add(part[1]);
                    }
                }

            }

            String[] limit = queryParams.get("limit");
            String[] offset = queryParams.get("offset");
            String[] order = queryParams.get("order");
            List<ColumnOrder> finalOrders = new LinkedList<>();
            if (order != null && order.length > 0 && order[0].length() > 0) {
                String[] orders = order[0].split(",");
                for (String s : orders) {
                    String[] parts = s.split(":");
                    if (tableData.getColumnList().contains(parts[0])) {
                        if (parts.length > 1) {

                            finalOrders.add(new ColumnOrder(parts[0], ColumnDirection.valueOf(parts[1])));
                        } else {
                            finalOrders.add(new ColumnOrder(parts[0], ColumnDirection.ASC));
                        }
                    }
                }

            }
            Integer actualLimit = 10;
            Integer actualOffset = 0;
            if (limit != null && limit.length > 0 && limit[0].length() > 0) {
                try {
                    actualLimit = Integer.valueOf(limit[0]);
                    if (actualLimit > 100) {
                        actualLimit = 100;
                    }
                } catch (NumberFormatException e) {

                }
            }

            if (offset != null && offset.length > 0 && offset[0].length() > 0) {
                try {
                    actualOffset = Integer.valueOf(offset[0]);
                } catch (NumberFormatException e) {

                }
            }


            String columns = String.join(",", columnNames);
            return paginatedResult("select ", columns, " from " + tableName, whereColumns, whereValues, finalOrders, actualOffset, actualLimit);
        } catch (SQLException e) {
            logger.error("SQL Exception ", e);
            error("Failed to get columns of table[" + tableName + "]", e);
        }
        return "{}";
    }

    @Override
    protected Integer getTotalCount() throws SQLException {
        Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("select count(*) from " + tableName);
        ResultSet rs = preparedStatement.executeQuery();
        rs.next();

        int anInt = rs.getInt(1);
        rs.close();
        preparedStatement.close();
        connection.close();
        return anInt;
    }

    @Override
    protected void init() throws SQLException {
        this.tableName = this.root;
        this.tableData = new TableData();
        initColumnData();

        Connection connection = this.dataSource.getConnection();
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        connection.close();
        ResultSet rs = databaseMetaData.getColumns(null, null, tableName, null);
//        debugColumnNames(rs);
        List<String> columnNames = getSingleColumnFromResultSet(rs, "COLUMN_NAME");

        Map<String, Boolean> found = new HashMap<>();
        for (String col : AutoColumns.keySet()) {
            found.put(col, false);
        }

        columnNames.stream().filter(found::containsKey).forEach(columnName -> {
            found.put(columnName, true);
        });


        if (!UserTables.contains(tableName)) {


            for (Map.Entry<String, Boolean> stringBooleanEntry : found.entrySet()) {
                if (stringBooleanEntry.getValue()) {
                    continue;
                }
                error("Column %s not found in table %s", stringBooleanEntry.getKey(), tableName);
                final List<String> sqlList = AutoColumns.get(stringBooleanEntry.getKey());

                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
                for (String sql : sqlList) {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    info("Executing: " + sql);
                    ps.execute();
                    ps.close();
                }
                connection.commit();
                connection.setAutoCommit(true);
                connection.close();

            }
        }

        rs.close();
        tableData.setColumnList(columnNames);
    }
}
