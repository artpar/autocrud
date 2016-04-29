package io.artpar.curd;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.jersey.server.ContainerRequest;

import javax.sql.DataSource;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * Created by parth on 30/4/16.
 */
public class TableController extends AbstractController {


    private Map<String, List<String>> ColumnsWeDontUpdate;

    private String tableName;
    private TableData tableData;
    private static final Random rng = new Random(new Date().getTime());


    public TableController(String tableName, DataSource dataSource, ObjectMapper objectMapper) throws SQLException, NoSuchMethodException {
        super(tableName, dataSource, objectMapper);

        final Method router = TableController.class.getMethod("router", ContainerRequestContext.class);
        this.rootResource.addMethod("GET").produces(MediaType.APPLICATION_JSON_TYPE).handledBy(this, router);
        this.rootResource.addMethod("POST").produces(MediaType.APPLICATION_JSON_TYPE).handledBy(this, router);

    }

    public void initColumnData() {
        ColumnsWeDontUpdate = new HashMap<>();
        ColumnsWeDontUpdate.put("id", Arrays.asList("alter table " + tableName + " add column `id` int(11) unsigned  primary key auto_increment"));
        ColumnsWeDontUpdate.put("created_at", Arrays.asList("alter table " + tableName + " add column created_at timestamp default CURRENT_TIMESTAMP"));
        ColumnsWeDontUpdate.put("updated_at", Arrays.asList("alter table " + tableName + " add column updated_at timestamp null default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP"));
        ColumnsWeDontUpdate.put("reference_id", Arrays.asList("alter table " + tableName + " add column reference_id varchar(50)",
                "update " + tableName + " set reference_id = uuid() where reference_id = null ",
                "alter table " + tableName + " add constraint "+tableName+"_reference_id_uniq_"+rng.nextInt(10000 - 1) +" unique(reference_id)"));

    }

    public Object router(ContainerRequestContext containerRequestContext) throws IOException, SQLException {
        final String path = containerRequestContext.getUriInfo().getPath();
        final int lastSlash = path.lastIndexOf("/");
        String ourName = path.substring(lastSlash + 1);
        switch (containerRequestContext.getMethod().toLowerCase()) {
            case "get":
                return this.list(containerRequestContext);
            case "post":
                return this.newItem(containerRequestContext);
        }
        return Response.status(Response.Status.NOT_IMPLEMENTED);
    }


    public Object newItem(ContainerRequestContext containerRequestContext) throws IOException, SQLException {
        InputStream is = containerRequestContext.getEntityStream();
        Map values = objectMapper.readValue(is, Map.class);
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
//        ResultSet rs = ps.getGeneratedKeys();
//        Integer id = rs.getInt(1);
//        values.put("id", id);
        values.put("reference_id", referenceId);
        ps.close();
        connection.close();

        return values;
    }

    public Object list(ContainerRequestContext containerRequestContext) {
        Collection<String> propertyNames = containerRequestContext.getPropertyNames();
        String query = ((ContainerRequest) containerRequestContext).getRequestUri().getQuery();

        try {
            if (tableData.getColumnList() == null) {
            }


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
            debug("%s", propertyNames);
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
            return paginatedResult("select ", columns, " from " + tableName, finalOrders, actualOffset, actualLimit);
        } catch (SQLException e) {
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
        for (String col : ColumnsWeDontUpdate.keySet()) {
            found.put(col, false);
        }

        columnNames.stream().filter(found::containsKey).forEach(columnName -> {
            found.put(columnName, true);
        });


        for (Map.Entry<String, Boolean> stringBooleanEntry : found.entrySet()) {
            if (stringBooleanEntry.getValue()) {
                continue;
            }
            error("Column %s not found in table %s", stringBooleanEntry.getKey(), tableName);
            final List<String> sqlList = ColumnsWeDontUpdate.get(stringBooleanEntry.getKey());

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


        rs.close();
        tableData.setColumnList(columnNames);
    }
}
