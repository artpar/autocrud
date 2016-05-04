package io.artpar.curd;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.beanutils.BasicDynaBean;
import org.apache.commons.beanutils.DynaClass;
import org.glassfish.jersey.server.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

/**
 * Created by parth on 30/4/16.
 */
public abstract class AbstractController {
    protected final String root;
    protected final DataSource dataSource;
    //    protected final DatabaseMetaData databaseMetaData;
    protected final ObjectMapper objectMapper;
    protected final Map<String, TableData> tableNames = new HashMap<>();
    protected final Logger logger = LoggerFactory.getLogger(SchemaController.class);
    protected Resource.Builder rootResource;
    protected String context;
    protected Map<String, AbstractTableController.ForeignKey> foreignKeyMap = new HashMap<>();


    public AbstractController(String context, String root, DataSource dataSource, ObjectMapper objectMapper) throws SQLException, NoSuchMethodException {
        this.context = context + root;
        this.root = root;
        this.rootResource = Resource.builder();
        this.rootResource.name("Route for [" + root + "]");
        this.rootResource.path(this.root);
        this.dataSource = dataSource;
        this.objectMapper = objectMapper;
        objectMapper.addMixIn(BasicDynaBean.class, SchemaController.DynaJsonMixin.class);
    }

    public abstract boolean isPermissionOk(boolean b, UserInterface userInterface, Map obj);

    public TableResult paginatedResult(String columns, String restOfTheClause,
                                       List<String> whereColumns, List<Object> whereValues,
                                       List<ColumnOrder> orderColumns, Integer offset, Integer limit, String[] childrenColumns, UserInterface userInterface)
            throws SQLException {

        String restOfTheClauseWithWhereClause = restOfTheClause;
        String tableName = root.split("/")[0];
        if (whereColumns.size() > 0) {
            restOfTheClauseWithWhereClause = restOfTheClause + " where " + tableName + ".status != 'deleted' and " + keyValuePairSeparatedBy(whereColumns, " and ");
        } else {
            restOfTheClauseWithWhereClause = restOfTheClause + " where " + tableName + ".status != 'deleted' ";
        }
        String countQuery = "select count(*) " + restOfTheClauseWithWhereClause;
        int filteredCount = getInt(countQuery, whereValues);
        int totalCount = getInt("select count(*) " + restOfTheClause, new LinkedList<>());
        String beforeLimitQuery = "select " + columns + restOfTheClauseWithWhereClause;
        if (orderColumns.size() > 0) {
            beforeLimitQuery = beforeLimitQuery + " order by  " + join(",", orderColumns);
        }
        List data = getList(beforeLimitQuery + " limit " + String.valueOf(offset) + "," + String.valueOf(limit), whereValues);
        List allowed = new LinkedList<>();
        for (Object o : data) {
            Map m = (Map) o;
            if (isPermissionOk(true, userInterface, m)) {
                allowed.add(o);
            }
        }


        if (childrenColumns.length > 0) {
            for (String childrenColumn : childrenColumns) {
                for (Object row : data) {
                    Map map = (Map) row;
                    AbstractTableController.ForeignKey fk = foreignKeyMap.get(childrenColumn);
                    List<Map<String, Object>> object = referenceIdToObject(fk.getReferenceTableName(), "reference_id", (String) map.get(childrenColumn));
                    map.put(fk.getReferenceTableName(), object);
                }
            }
        }

        TableResult tr = new TableResult();
        tr.setFilteredCount(filteredCount);
        tr.setOffset(offset);
        tr.setSize(data.size());
        tr.setData(allowed);
        tr.setTotalCount(totalCount);
        return tr;
    }

    private int getInt(String countQuery, List<Object> whereValues) throws SQLException {
        logger.debug("Execute count query: " + countQuery);
        Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(countQuery);
        for (int i = 0; i < whereValues.size(); i++) {
            Object whereValue = whereValues.get(i);
            preparedStatement.setObject(i + 1, whereValue);
        }

        ResultSet rs = preparedStatement.executeQuery();
        rs.next();
        int filteredCount = rs.getInt(1);
        rs.close();
        preparedStatement.close();
        connection.close();
        return filteredCount;
    }

    private String keyValuePairSeparatedBy(List<String> whereColumns, String a) {
        String s = "";
        int lastSecond = whereColumns.size() - 1;
        for (int i = 0; i < whereColumns.size(); i++) {
            String whereColumn = whereColumns.get(i);
            s = s + whereColumn + "=?";
            if (i < lastSecond) {
                s = s + a;
            }
        }
        return s;

    }

    private String join(String s, List items) {
        String q = "";
        for (int i = 0; i < items.size() - 2; i++) {
            q = q + items.get(i).toString() + s;
        }
        q = q + items.get(items.size() - 1).toString();
        return q;
    }

    protected abstract Integer getTotalCount() throws SQLException;

    /**
     * Parses a query string passed from the client to the
     * server and builds a <code>HashTable</code> object
     * with key-value pairs.
     * The query string should be in the form of a string
     * packaged by the GET or POST method, that is, it
     * should have key-value pairs in the form <i>key=value</i>,
     * with each pair separated from the next by a &amp; character.
     * <p/>
     * <p>A key can appear more than once in the query string
     * with different values. However, the key appears only once in
     * the hashtable, with its value being
     * an array of strings containing the multiple values sent
     * by the query string.
     * <p/>
     * <p>The keys and values in the hashtable are stored in their
     * decoded form, so
     * any + characters are converted to spaces, and characters
     * sent in hexadecimal notation (like <i>%xx</i>) are
     * converted to ASCII characters.
     *
     * @return a <code>HashTable</code> object built
     * from the parsed key-value pairs
     * @throws IllegalArgumentException if the query string is invalid
     */

    /*
         * Parse a name in the query string.
         */
    public Resource.Builder getRootResource() {
        return rootResource;
    }

    protected List getList(String sql, List<Object> questions) throws SQLException {
        logger.debug("Query for get list " + this.root);
        logger.debug(sql + "\n" + questions);
        Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql);
        for (int i = 0; i < questions.size(); i++) {
            Object whereValue = questions.get(i);
            statement.setObject(i + 1, whereValue);
        }
        ResultSet rs = statement.executeQuery();
        debugColumnNames(rs);
        printResultSet(rs);
        List<Map<String, Object>> r = getResults(rs);
//        RowSetDynaClass rsdc = new RowSetDynaClass(rs);
        rs.close();
        statement.close();
        connection.close();
//        return (List) rsdc.getRows().stream().map(new Function() {
//            @Override
//            public Object apply(Object o) {
//                return ((BasicDynaBean) o).getMap();
//            }
//        }).collect(Collectors.toList());
        return r;
    }

    protected abstract void init() throws SQLException, NoSuchMethodException;

    protected void error(Object... args) {
        Object arg = args[0];
        Object[] rem = new Object[args.length - 1];
        System.arraycopy(args, 1, rem, 0, args.length - 1);
        logger.error(String.format(String.valueOf(args[0]), rem));
    }

    protected void debug(Object... args) {
        Object arg = args[0];
        Object[] rem = new Object[args.length - 1];
        System.arraycopy(args, 1, rem, 0, args.length - 1);
        logger.debug(String.format((String) arg, rem));
    }

    protected void info(Object... args) {
        Object arg = args[0];
        Object[] rem = new Object[args.length - 1];
        System.arraycopy(args, 1, rem, 0, args.length - 1);
        logger.debug(String.format((String) arg, rem));
    }

    protected List<String> getSingleColumnFromResultSet(ResultSet rs, Integer colId) throws SQLException {
        List<String> result = new LinkedList<>();
        while (rs.next()) {
            result.add(rs.getString(colId));
        }
        return result;
    }

    protected List<List<Object>> getSingleColumnFromResultSet(ResultSet rs, String[] colNames) throws SQLException {
        List<List<Object>> result = new LinkedList<>();
        while (rs.next()) {
            List<Object> row = new LinkedList<>();
            for (int i = 0; i < colNames.length; i++) {
                String colName = colNames[i];
                row.add(rs.getString(colName));
            }
            result.add(row);

        }
        return result;
    }

    protected String[] debugColumnNames(ResultSet rs) throws SQLException {
        String[] result = new String[rs.getFetchSize()];
        ResultSetMetaData resultSetMetadata = rs.getMetaData();
        int columnCount = resultSetMetadata.getColumnCount();
        debug("%d columns in result for fetching columns", columnCount);
        for (int i = 1; i <= columnCount; i++) {
            debug("Column %d: %s", i, resultSetMetadata.getColumnName(i));
        }
        return result;
    }


    private List<Map<String, Object>> getResults(ResultSet rs) throws SQLException {

        List<Map<String, Object>> result = new LinkedList<>();
        List<String> columnNames = new LinkedList<>();

        ResultSetMetaData resultSetMetadata = rs.getMetaData();
        int columnCount = resultSetMetadata.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            columnNames.add(resultSetMetadata.getColumnLabel(i));
        }

        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 0; i < columnNames.size(); i++) {
                String columnName = columnNames.get(i);
                row.put(columnName, rs.getObject(i + 1));
            }
            result.add(row);
        }
        return result;
    }

    protected void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData resultSetMetadata = rs.getMetaData();
        int columnCount = resultSetMetadata.getColumnCount();

        if (logger.isDebugEnabled()) {

            logger.debug("{} columns in result for fetching columns", columnCount);
            for (int i = 1; i <= columnCount; i++) {
                System.out.printf("\t%s", resultSetMetadata.getColumnName(i));
            }
            System.out.println("");
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.printf("\t%s", rs.getObject(i));
                }
                System.out.println("");
            }
        }
        rs.beforeFirst();
    }


    protected List<Map<String, Object>> referenceIdToObject(String tableName, String columnName, String referenceId) throws SQLException {
        Connection conn = this.dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement("select * from " + tableName + " where " + columnName + " = ?");
        ps.setObject(1, referenceId);
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> res = getResults(rs);
        rs.close();
        ps.close();
        conn.close();
        return res;
    }

    protected Long referenceIdToId(String referenceTable, Object columnValue) throws SQLException {
        Connection conn = this.dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement("select id from " + referenceTable + " where reference_id = ?");
        ps.setObject(1, columnValue);
        ResultSet rs = ps.executeQuery();
        rs.next();
        long id = rs.getLong(1);
        rs.close();
        ps.close();
        conn.close();
        return id;
    }

    public abstract class DynaJsonMixin {
        @JsonIgnore
        public DynaClass dynaClass;

    }
}
