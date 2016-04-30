package io.artpar.curd;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.jersey.server.ContainerRequest;

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
public class TableController extends AbstractController {


    private static final List<String> UserTables = Arrays.asList("user", "user_usergroup", "usergroup");
    private static final Random rng = new Random(new Date().getTime());
    private Map<String, List<String>> AutoColumns;
    private String tableName;
    private TableData tableData;
    private int tablePermission;
    private Long userId;
    private Long userGroupId;
    private List<String> ColumnsWeDontUpdate = Arrays.asList("id", "reference_id", "created_at", "updated_at");


    public TableController(String context, String tableName, DataSource dataSource, ObjectMapper objectMapper) throws SQLException, NoSuchMethodException {
        super(context, tableName, dataSource, objectMapper);

        final Method router = TableController.class.getMethod("router", ContainerRequestContext.class);

        debug("Added GET " + this.context);
        this.rootResource.addMethod("GET").produces(MediaType.APPLICATION_JSON_TYPE).handledBy(this, router);

        debug("Added POST " + this.context);
        this.rootResource.addMethod("POST").produces(MediaType.APPLICATION_JSON_TYPE).handledBy(this, router);


        debug("Added PUT " + this.context);
        this.rootResource.addMethod("PUT").produces(MediaType.APPLICATION_JSON_TYPE).handledBy(this, router);

        debug("Added DELETE " + this.context);
        this.rootResource.addMethod("DELETE").produces(MediaType.APPLICATION_JSON_TYPE).handledBy(this, router);
    }

    public static int getNthDigit(int permission, int count) {
        int number = permission;
        while (count > 0) {
            count--;
            number = number / 10;
        }
        return number % 10;
    }

    public void initColumnData() {
        AutoColumns = new HashMap<>();
        AutoColumns.put("id", Arrays.asList("alter table " + tableName + " add column `id` int(11) unsigned  primary key auto_increment"));
        AutoColumns.put("created_at", Arrays.asList("alter table " + tableName + " add column created_at timestamp default CURRENT_TIMESTAMP"));
        AutoColumns.put("updated_at", Arrays.asList("alter table " + tableName + " add column updated_at timestamp null default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP"));
        AutoColumns.put("status", Arrays.asList("alter table " + tableName + " add column status varchar(20) default 'pending'"));
        AutoColumns.put("reference_id", Arrays.asList("alter table " + tableName + " add column reference_id varchar(50)",
                "update " + tableName + " set reference_id = uuid() where reference_id = null ",
                "alter table " + tableName + " add constraint " + tableName + "_reference_id_uniq_" + rng.nextInt(10000 - 1) + " unique(reference_id)"));
        AutoColumns.put("permission", Arrays.asList("alter table " + tableName + " add column permission int(4) not null default 755"));
        AutoColumns.put("user_id", Arrays.asList("alter table " + tableName + " add column user_id int(11)"
                , "update " + tableName + " set user_id = (select min(id) from user)"
                , "alter table " + tableName + " add constraint " + tableName + "_user_id_" + rng.nextInt(10000 - 1) + " foreign key (user_id) references user(id) "));
        AutoColumns.put("usergroup_id", Arrays.asList("alter table " + tableName + " add column usergroup_id int(11)"
                , "update " + tableName + " set usergroup_id = (select min(id) from usergroup)"
                , "alter table " + tableName + " add constraint " + tableName + "_usergroup_id_" + rng.nextInt(10000 - 1) + " foreign key (usergroup_id) references usergroup(id) "));
    }

    public Object router(ContainerRequestContext containerRequestContext) throws IOException, SQLException {
        final String path = containerRequestContext.getUriInfo().getPath();
        final int lastSlash = path.lastIndexOf("/");
        String ourName = path.substring(lastSlash + 1);
        final MyRequest myRequest = new MyRequest(containerRequestContext);
        Object re = permissionCheck(myRequest);
        if (re != null) {
            return re;
        }
        switch (containerRequestContext.getMethod().toLowerCase()) {
            case "get":
                return this.list(myRequest);
            case "post":
                return this.newItem(myRequest);
            case "put":
                return this.updateItem(myRequest);
            case "delete":
                return this.deleteItem(myRequest);
        }
        return Response.status(Response.Status.NOT_IMPLEMENTED);
    }

    private Object permissionCheck(MyRequest myRequest) throws IOException {
        if (Objects.equals(tableName, "user")) {
            return null;
        }
        UserInterface userInterface = myRequest.getUser();

        final boolean isGet = myRequest.getMethod().equalsIgnoreCase("get");
        boolean ok1 = isOk(isGet, userInterface, tablePermission, userId, userGroupId);
        if (!ok1) {
            return Response.status(Response.Status.UNAUTHORIZED);
        }


        String referenceId = null;
        switch (myRequest.getMethod().toLowerCase()) {
            case "get":
                referenceId = myRequest.getQueryParam("reference_id");
                break;
            case "put":
            case "post":
            case "delete":
                referenceId = (String) myRequest.getBodyValueMap().get("reference_id");
        }


        final MultivaluedHashMap<String, String> values = new MultivaluedHashMap<>();
        values.putSingle("reference_id", referenceId);
        Object res = getResult(values, userInterface);
        if (res instanceof TableResult) {
            for (Object o : ((TableResult) res).getData()) {
                Map map = (Map)o;
                int permission = (int) map.get("permission");

                Long ownerUserId = 1L;
                try {

                    ownerUserId = Long.valueOf( String.valueOf( map.get("user_id")) );
                }catch (Exception e) {

                }
                Long ownerGroupId = 1L;
                try {

                    ownerGroupId = Long.valueOf(String.valueOf( map.get("usergroup_id") ));
                }catch (Exception e) {

                }
                if (isOk(isGet, userInterface, permission, ownerUserId, ownerGroupId)) {
                    return null;
                }
            }
            return Response.status(Response.Status.NOT_FOUND);
        } else {
        }

        return null;
    }

    private boolean isOk(boolean isGet, UserInterface userInterface, int permission, Long ownerUserId, Long ownerGroupId) {
        boolean canRead = false, canWrite = false;
        int checkAgainst = getUserCurrentPermissionValue(userInterface, permission, ownerUserId, ownerGroupId);
        if ((checkAgainst & 1) == 1) {
            canRead = true;
        }
        if ((checkAgainst & 2) == 2) {
            canWrite = true;
        }

        return ((canWrite && !isGet) || (canRead && isGet));
    }

    private int getUserCurrentPermissionValue(UserInterface userInterface, int permission, Long ownerUserId, Long ownerGroupId) {
//        boolean isUser = false, isGroup = false;
        UserType type = UserType.World;
        int count = 0;

        if (Objects.equals(ownerUserId, userInterface.getId())) {
            type = UserType.User;
            count = 2;
        } else if (userInterface.getUserGroupId().contains(ownerGroupId)) {
            type = UserType.Group;
            count = 1;
        }
        return getNthDigit(permission, count);
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
        MultivaluedMap<String, String> queryParams = containerRequestContext.getQueryParameters();
        return getResult(queryParams, containerRequestContext.getUser());
    }

    private Object getResult(MultivaluedMap<String, String> queryParams, UserInterface userInterface) {
        try {
            List<String> columnNames = tableData.getColumnList();
            List<String> finalList = new LinkedList<>();
            String columnFilterList = queryParams.getFirst("column");
            if (columnFilterList != null && columnFilterList.length() > 0) {
                String columnInRequest = queryParams.getFirst("column");
                List<String> names = Arrays.asList(columnInRequest.split(","));
                for (String name : columnNames) {
                    if (names.contains(name)) {
                        finalList.add(name);
                    }
                }
                columnNames = finalList;
            }

            String where = queryParams.getFirst("where");
            List<String> whereColumns = new LinkedList<>();
            List<Object> whereValues = new LinkedList<>();
            if (where != null && where.length() > 3) {
                String[] whereList = where.split(",");
                for (String s : whereList) {
                    String[] part = s.split(":", 2);
                    if (part.length == 2 && tableData.getColumnList().contains(part[0])) {
                        whereColumns.add(part[0]);
                        whereValues.add(part[1]);
                    }
                }

            }

            String limit = queryParams.getFirst("limit");
            String offset = queryParams.getFirst("offset");
            String order = queryParams.getFirst("order");
            List<ColumnOrder> finalOrders = new LinkedList<>();
            if (order != null && order.length() > 0) {
                String[] orders = order.split(",");
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
            if (limit != null && limit.length() > 0) {
                try {
                    actualLimit = Integer.valueOf(limit);
                    if (actualLimit > 100) {
                        actualLimit = 100;
                    }
                } catch (NumberFormatException ignored) {

                }
            }

            if (offset != null && offset.length() > 0) {
                try {
                    actualOffset = Integer.valueOf(offset);
                } catch (NumberFormatException ignored) {

                }
            }


            String columns = String.join(",", columnNames);
            return paginatedResult(columns, " from " + tableName, whereColumns, whereValues, finalOrders, actualOffset, actualLimit, userInterface);
        } catch (SQLException e) {
            logger.error("SQL Exception ", e);
            error("Failed to get columns of table[" + tableName + "]", e);
        }
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR);
    }

    @Override
    public boolean isPermissionOk(boolean isGet, UserInterface userInterface, Map obj) {
        int permission = (int) obj.get("permission");
        Object user_id = obj.get("user_id");
        if (user_id == null )  {
            user_id = 1L;
        }
        Object usergroup_id = obj.get("usergroup_id");
        if (usergroup_id == null) {
            usergroup_id = 1L;
        }
        return isOk(isGet, userInterface, permission, (Long) user_id, (Long) usergroup_id);
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


    public void checkTableExist() throws SQLException {
        Connection conn = dataSource.getConnection();
        ResultSet table = conn.getMetaData().getTables(null, null, tableName, null);
        boolean ok = table.next();
        table.close();
        conn.close();
        if (!ok) {
            logger.error("Table [" + tableName + "] does not exist. Creating it.");
            conn = dataSource.getConnection();
            PreparedStatement ps = conn.prepareStatement("create table " + tableName + " ( " +
                    "id int(11) AUTO_INCREMENT PRIMARY KEY, " +
                    "name VARCHAR(50) not NULL , " +
                    "user_id int(11) default 1 REFERENCES user(id), " +
                    "usergroup_id int(11) DEFAULT 1 REFERENCES usergroup (id), " +
                    "reference_id varchar(50) not null UNIQUE)"
            );
            ps.execute();
            ps.close();
            conn.close();

        } else {
            logger.info("Table [" + tableName + "] is ok.");
        }
    }

    @Override
    protected void init() throws SQLException {
        this.tableName = this.root;
        this.tableData = new TableData();
        checkTableExist();
        initWorldTable();
        initColumnData();

        Connection connection = this.dataSource.getConnection();
        DatabaseMetaData databaseMetaData = connection.getMetaData();
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
                    debug("Executing: " + sql);
                    ps.execute();
                    ps.close();
                }
                connection.commit();
                connection.setAutoCommit(true);
                connection.close();

            }
        }

        connection.close();
        rs.close();
        tableData.setColumnList(columnNames);
    }

    private void initWorldTable() throws SQLException {
        Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement("SELECT * FROM world WHERE name = ?");
        ps.setString(1, tableName);
        ResultSet rs = ps.executeQuery();
        boolean ok = rs.next();
        if (!ok) {
            PreparedStatement ps1 = conn.prepareStatement("INSERT INTO world (name, reference_id, user_id, usergroup_id, status, permission) VALUES (?,?,1,1,'active',755)");
            ps1.setString(1, tableName);
            ps1.setString(2, UUID.randomUUID().toString());
            ps.execute();
            ps.close();
            rs.close();
            conn.close();
            tablePermission = 755;
            userId = 1L;
            userGroupId = 1L;
            logger.info("Table info does not exist in world for " + tableName + ". Creating it");
        } else {
            tablePermission = rs.getInt("permission");
            userId = rs.getLong("user_id");
            userGroupId = rs.getLong("usergroup_id");
            logger.info("The world knows about this table");
        }

    }

    class MyRequest {
        private Map map;
        private Map<String, Object> originalValue = new HashMap<>();
        private ContainerRequestContext containerRequestContext;

        public MyRequest(ContainerRequestContext containerRequestContext) throws IOException {
            this.containerRequestContext = containerRequestContext;
            InputStream is = containerRequestContext.getEntityStream();
            if (is != null) {
                try {
                    map = objectMapper.readValue(is, Map.class);
                    originalValue.putAll(map);
                    List<String> removeKey = new LinkedList<>();
                    for (Object o : map.keySet()) {
                        String key = (String) o;
                        if (ColumnsWeDontUpdate.contains(key)) {
                            removeKey.add(key);
                        }
                    }
                    for (String s : removeKey) {
                        map.remove(s);
                    }
                } catch (JsonMappingException e) {
                    map = null;
                }
            } else {
            }
        }

        public UserInterface getUser() {
            return (UserInterface) containerRequestContext.getProperty("user");
        }

        public Map getBodyValueMap() throws IOException {
            return map;
        }

        public UriInfo getUriInfo() {
            return containerRequestContext.getUriInfo();
        }

        public String getMethod() {
            return containerRequestContext.getMethod();
        }

        public URI getRequestUri() {
            return ((ContainerRequest) containerRequestContext).getRequestUri();
        }

        public Object getOriginalValue(String key) {
            return originalValue.get(key);
        }

        public String getQueryParam(String key) {
            return containerRequestContext.getUriInfo().getQueryParameters().getFirst(key);
        }

        public MultivaluedMap<String, String> getQueryParameters() {
            return containerRequestContext.getUriInfo().getQueryParameters();
        }

    }
}
