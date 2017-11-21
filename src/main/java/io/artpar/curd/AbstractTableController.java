package io.artpar.curd;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.server.ContainerRequest;

import javax.annotation.security.RolesAllowed;
import javax.sql.DataSource;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * Created by parth on 2/5/16.
 */
public abstract class AbstractTableController extends AbstractController {
    private static final List<String> UserTables = Arrays.asList("user_usergroup");
    private static final Random rng = new Random(new Date().getTime());
    protected String tableName;
    protected TableData tableData;
    private Map<String, List<String>> AutoColumns;
    private int tablePermission;
    private String userId;
    private String userGroupId;
    private List<String> ColumnsWeDontUpdate = Arrays.asList("id", "reference_id", "created_at", "updated_at");
    private String referenceId;

    public AbstractTableController(String tableName, String context, String root, DataSource dataSource, ObjectMapper objectMapper) throws SQLException, NoSuchMethodException {
        super(context, root, dataSource, objectMapper);
        this.tableName = tableName;
        init();
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
                , "update " + tableName + " set user_id = (select id from (select min(id) id from user) x )"
                , "alter table " + tableName + " add constraint " + tableName + "_user_id_" + rng.nextInt(10000 - 1) + " foreign key (user_id) references user(id) "));
        AutoColumns.put("usergroup_id", Arrays.asList("alter table " + tableName + " add column usergroup_id int(11)"
                , "update " + tableName + " set usergroup_id = (select id from ( select min(id) id from usergroup) x )"
                , "alter table " + tableName + " add constraint " + tableName + "_usergroup_id_" + rng.nextInt(10000 - 1) + " foreign key (usergroup_id) references usergroup(id) "));
    }

    @RolesAllowed("ROLE_USER")
    public abstract Object router(ContainerRequestContext containerRequestContext) throws IOException, SQLException;

    protected Object permissionCheck(MyRequest myRequest) throws IOException {
        UserInterface userInterface = myRequest.getUser();

        final boolean isGet = myRequest.getMethod().equalsIgnoreCase("get");
        boolean ok1 = isOk(isGet, userInterface, tablePermission, userId, userGroupId);
        if (!ok1) {
            return Response.status(Response.Status.UNAUTHORIZED);
        }


        String referenceId = null;
        switch (myRequest.getMethod().toLowerCase()) {
            case "get":
                referenceId = myRequest.getQueryParam("referenceId");
                if (referenceId == null || referenceId.length() < 1) {
                    return null;
                }
                break;
            case "post":
                return null;
            case "put":
            case "delete":
                referenceId = (String) myRequest.getOriginalValue("referenceId");
                if (referenceId == null || referenceId.length() < 1) {
                    referenceId = (String) myRequest.getOriginalValue("reference_id");
                }
        }


        final MultivaluedHashMap<String, String> values = new MultivaluedHashMap<>();
        values.putSingle("where", "reference_id:" + referenceId);
        Object res = getResult(values, userInterface);
        if (res instanceof TableResult && ((TableResult) res).getData().size() > 0) {
            for (Object o : ((TableResult) res).getData()) {
                Map map = (Map) o;
                int permission = (int) map.get("permission");

                String ownerUserId = (String) map.get("user_id");
                String ownerGroupId = (String) map.get("usergroup_id");
                if (isOk(isGet, userInterface, permission, ownerUserId, ownerGroupId)) {
                    return null;
                }
            }
            return Response.status(Response.Status.UNAUTHORIZED).build();
        } else {
            return Response.status(Response.Status.BAD_REQUEST).entity(res).build();
        }

    }

    private boolean isOk(boolean isGet, UserInterface userInterface, int permission, Object ownerUserId, Object ownerGroupId) {
        boolean canRead = false, canWrite = false;
        int checkAgainst = getUserCurrentPermissionValue(userInterface, permission, ownerUserId, ownerGroupId);
        if ((checkAgainst & 4) == 4) {
            canRead = true;
        }
        if ((checkAgainst & 2) == 2) {
            canWrite = true;
        }

        return ((canWrite && !isGet) || (canRead && isGet));
    }

    private int getUserCurrentPermissionValue(UserInterface userInterface, int permission, Object ownerUserId, Object ownerGroupId) {
//        boolean isUser = false, isGroup = false;
        UserType type = UserType.World;
        int count = 0;

        if (Objects.equals(ownerUserId, userInterface.getId())) {
            type = UserType.User;
            count = 2;
        } else if (userInterface.getGroupIdsOfUser().contains(ownerGroupId)) {
            type = UserType.Group;
            count = 1;
        }
        return getNthDigit(permission, count);
    }

    protected Object getResult(MultivaluedMap<String, String> queryParams, UserInterface userInterface) {
        try {
            StringBuilder from = new StringBuilder(" from " + tableName);

            List<String> columnNames = tableData.getColumnList();

            logger.info("Columns to select: [{}]", StringUtils.join(columnNames, ", "));
            if (!columnNames.contains("permission")) {
                columnNames.add("user.permission");
            }

            List<String> finalList = new LinkedList<>();
            String columnFilterList = queryParams.getFirst("column");
            if (columnFilterList != null && columnFilterList.length() > 0) {
                List<String> requestedColumnNames = Arrays.asList(columnFilterList.split(","));
                for (String name : columnNames) {
                    if (requestedColumnNames.contains(name)) {
                        finalList.add(name);
                    }
                }
                columnNames = finalList;
            }


            finalList = new LinkedList<>();
            for (String name : columnNames) {
                if (foreignKeyMap.containsKey(name)) {
                    final ForeignKey fk = foreignKeyMap.get(name);
                    from.append(" join ").append(fk.getReferenceTableName()).append(" on ").append(tableName).append(".").append(name).append("=")
                        .append(fk.getReferenceTableName()).append(".").append(fk.getReferenceColumnName());
                    finalList.add(fk.getReferenceTableName() + "." + "reference_id" + " as " + name);
                } else if (!name.contains(".")) {
                    finalList.add(tableName + "." + name);
                }
            }
            columnNames = finalList;


            // col1:val1,col2:val2
            String where = queryParams.getFirst("where");
            List<String> whereColumns = new LinkedList<>();
            List<Object> whereValues = new LinkedList<>();
            if (where != null && where.length() > 3) {
                String[] whereList = where.split(",");
                for (String s : whereList) {
                    String[] part = s.split(":", 2);
                    if (part.length == 2 && tableData.getColumnList().contains(part[0])) {
                        whereColumns.add(tableName + "." + part[0]);
                        Object e = part[1];
                        if (foreignKeyMap.containsKey(part[0])) {
                            ForeignKey fk = foreignKeyMap.get(part[0]);
                            e = referenceIdToId(fk.getReferenceTableName(), e);
                        } else if (Objects.equals(part[0], "user_id")) {
                            e = referenceIdToId("user", e);
                        }
                        whereValues.add(e);
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
                            finalOrders.add(new ColumnOrder(tableName + "." + parts[0], ColumnDirection.valueOf(parts[1].toLowerCase())));
                        } else {
                            finalOrders.add(new ColumnOrder(tableName + "." + parts[0], ColumnDirection.asc));
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

            String[] childrenColumns = new String[]{};
            if (queryParams.containsKey("children")) {
                String children = queryParams.getFirst("children");
                childrenColumns = children.split(",");
            }


            String columns = String.join(",", columnNames);
            return paginatedResult(columns, from.toString(), whereColumns, whereValues, finalOrders, actualOffset, actualLimit, childrenColumns, userInterface);
        } catch (SQLException e) {
            logger.error("SQL Exception ", e);
            error("Failed to get columns of table[" + tableName + "]", e);
        }
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Failed").build();
    }

    @Override
    public boolean isPermissionOk(boolean isGet, UserInterface userInterface, Map obj) {

        Object permission1 = obj.get("permission");
        if (permission1 == null) {
            permission1 = 755;
        }

        int permission = (int) permission1;
        Object user_id = obj.get("user_id");
        Object usergroup_id = obj.get("usergroup_id");
        return isOk(isGet, userInterface, permission, user_id, usergroup_id);
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
            PreparedStatement ps;
            if (tableName.equalsIgnoreCase("world")) {

                ps = conn.prepareStatement("create table " + tableName + " ( " +
                        "id int(11) AUTO_INCREMENT PRIMARY KEY, " +
                        "name VARCHAR(50) not NULL , " +
                        "user_id int(11) default 1 REFERENCES user(id), " +
                        "default_permission int(4) default 755, " +
                        "usergroup_id int(11) DEFAULT 1 REFERENCES usergroup (id), " +
                        "reference_id varchar(50) not null UNIQUE)"
                );
            } else {
                ps = conn.prepareStatement("create table " + tableName + " ( " +
                        "id int(11) AUTO_INCREMENT PRIMARY KEY, " +
                        "name VARCHAR(50) not NULL , " +
                        "user_id int(11) default 1 REFERENCES user(id), " +
                        "usergroup_id int(11) DEFAULT 1 REFERENCES usergroup (id), " +
                        "reference_id varchar(50) not null UNIQUE)"
                );

            }
            ps.execute();
            ps.close();
            conn.close();

        } else {
            logger.info("Table [" + tableName + "] is ok.");
        }
    }

    @Override
    protected void init() throws SQLException {
        this.tableData = new TableData();
        checkTableExist();
        initWorldTable();
        initColumnData();


        Connection connection1 = this.dataSource.getConnection();
        DatabaseMetaData databaseMetaData = connection1.getMetaData();
        ResultSet rs = databaseMetaData.getColumns(null, null, tableName, null);

        debugColumnNames(rs);
        printResultSet(rs);
        rs.beforeFirst();
        List<List<Object>> columnNamesObject = getSingleColumnFromResultSet(rs, new String[]{"COLUMN_NAME"});
        List<String> columnNames = new LinkedList<>();
        for (List<Object> objects : columnNamesObject) {
            columnNames.add((String) objects.get(0));
        }

        rs.close();
        ResultSet keys = databaseMetaData.getImportedKeys(null, null, tableName);
        debugColumnNames(keys);
        printResultSet(keys);
        keys.beforeFirst();
        while (keys.next()) {
            String thatTableName = keys.getString("PKTABLE_NAME");
            String thatColumnName = keys.getString("PKCOLUMN_NAME");
            String thisColumnName = keys.getString("FKCOLUMN_NAME");
            ForeignKey fk = new ForeignKey(thatColumnName, thatTableName);
            foreignKeyMap.put(thisColumnName, fk);
        }


        connection1.close();

        Map<String, Boolean> found = new HashMap<>();
        for (String col : AutoColumns.keySet()) {
            found.put(col, false);
        }

        columnNames.stream().filter(found::containsKey).forEach(columnName -> {
            found.put(columnName, true);
        });


        if (!UserTables.contains(tableName) && found.size() > 0) {


            for (Map.Entry<String, Boolean> stringBooleanEntry : found.entrySet()) {
                if (stringBooleanEntry.getValue()) {
                    continue;
                }
                error("Column %s not found in table %s", stringBooleanEntry.getKey(), tableName);
                final List<String> sqlList = AutoColumns.get(stringBooleanEntry.getKey());

                Connection connection = dataSource.getConnection();
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

//        idColumnCheck();
        tableData.setColumnList(columnNames);
    }

    private void idColumnCheck() throws SQLException {
        if (tableName.equalsIgnoreCase("user_usergroup")) {
            return;
        }
        Connection conn = this.dataSource.getConnection();
        ResultSet idColRes = conn.getMetaData().getColumns(null, "id", tableName, null);
        idColRes.next();
        String dataType = idColRes.getString("DATA_TYPE");
        if (!dataType.equalsIgnoreCase("int(11) unsigned")) {
            logger.error("[" + tableName + "] Id column is not of type int(11) unsigned. Changing it");
            PreparedStatement ps = conn.prepareStatement("alter table " + tableName + " modify id int(11) unsigned auto_increment");
            ps.execute();
            ps.close();
        }
        idColRes.close();
        conn.close();
    }

    private void initWorldTable() throws SQLException {
        try {

            Connection conn = dataSource.getConnection();
            ResultSet rs;
//            if (tableName.equalsIgnoreCase("world")) {
//                rs = conn.getMetaData().getTables(null, null, tableName, null);
//            } else {
            PreparedStatement ps = conn.prepareStatement("SELECT `w`.`id`,\n" +
                    "    `w`.`name`,\n" +
                    "    u.reference_id AS user_id,\n" +
                    "    `w`.`default_permission`,\n" +
                    "    ug.reference_id AS usergroup_id,\n" +
                    "    `w`.`reference_id`,\n" +
                    "    `w`.`updated_at`,\n" +
                    "    `w`.`created_at`,\n" +
                    "    `w`.`permission`,\n" +
                    "    `w`.`status`\n" +
                    "FROM `inf`.`world` w JOIN  user u ON w.user_id = u.id JOIN usergroup ug ON ug.id = w.usergroup_id WHERE w.name = ?");
            ps.setString(1, tableName);
            rs = ps.executeQuery();
//            }


            boolean ok = rs.next();
            if (!ok) {
                logger.info("Table info does not exist in world for " + tableName + ". Creating it");
                PreparedStatement ps1 = conn.prepareStatement("INSERT INTO world (name, reference_id, user_id, usergroup_id, status, permission) VALUES (?,?,1,1,'active',755)");
                ps1.setString(1, tableName);
                ps1.setString(2, UUID.randomUUID().toString());
                ps1.execute();
                ps1.close();
                tablePermission = 755;
                userId = "";
                userGroupId = "";
            } else {
                if (!tableName.equalsIgnoreCase("world")) {
                    tablePermission = rs.getInt("permission");
                    userId = rs.getString("user_id");
                    userGroupId = rs.getString("usergroup_id");
                    referenceId = rs.getString("reference_id");
                    logger.info("The world knows about this table");
                } else {
                    tablePermission = 755;
                    userId = "";
                    userGroupId = "";
                    logger.info("The world is visible now");
                }
            }
            rs.close();
//            ps.close();
            conn.close();

        } catch (Exception t) {
            throw new RuntimeException("For table " + tableName, t);
        }

    }

    public static class ForeignKey {
        String referenceTableName;
        String referenceColumnName;

        public ForeignKey(String thatColumnName, String thatTableName) {

            this.referenceColumnName = thatColumnName;
            this.referenceTableName = thatTableName;
        }

        public String getReferenceTableName() {
            return referenceTableName;
        }

        public void setReferenceTableName(String referenceTableName) {
            this.referenceTableName = referenceTableName;
        }

        public String getReferenceColumnName() {
            return referenceColumnName;
        }

        public void setReferenceColumnName(String referenceColumnName) {
            this.referenceColumnName = referenceColumnName;
        }

        @Override
        public String toString() {
            return " references " + referenceTableName + "(" + referenceColumnName + ")";
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
                    try {
                        map = objectMapper.readValue(is, Map.class);
                    } catch (JsonParseException e) {
                        return;
                    }
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
            if (map == null) {
                map = new HashMap<>();
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
