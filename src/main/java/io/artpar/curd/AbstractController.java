package io.artpar.curd;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.beanutils.BasicDynaBean;
import org.apache.commons.beanutils.DynaClass;
import org.apache.commons.beanutils.RowSetDynaClass;
import org.glassfish.jersey.server.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by parth on 30/4/16.
 */
public abstract class AbstractController {
    protected final String root;
    protected final DataSource dataSource;
//    protected final DatabaseMetaData databaseMetaData;
    protected final ObjectMapper objectMapper;
    protected final Map<String, TableData> tableNames = new HashMap<>();
    private final Logger logger = LoggerFactory.getLogger(SchemaController.class);
    protected Resource.Builder rootResource;



    public TableResult paginatedResult(String select, String columns, String restOfTheClause, List<ColumnOrder> orderColumns, Integer offset, Integer limit) throws SQLException {
        String countQuery = "select count(*) " + restOfTheClause;
        Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(countQuery);
        ResultSet rs = preparedStatement.executeQuery();
        rs.next();
        int filteredCount = rs.getInt(1);
        rs.close();
        preparedStatement.close();
        connection.close();
        String beforeLimitQuery = "select " + columns + restOfTheClause;
        if (orderColumns.size() > 0) {
            beforeLimitQuery = beforeLimitQuery + " order by  " + join(",", orderColumns);
        }
        List data = getList(beforeLimitQuery + " limit " + String.valueOf(offset) + "," + String.valueOf(limit));

        TableResult tr = new TableResult();
        tr.setFilteredCount(filteredCount);
        tr.setOffset(offset);
        tr.setSize(limit);
        tr.setData(data);
        tr.setTotalCount(getTotalCount());
        return tr;
    }

    private String join (String s, List items) {
        String q = "";
        for (int i = 0;i < items.size() - 2; i++) {
            q = q +  items.get(i).toString() + s;
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
     * <p>
     * <p>A key can appear more than once in the query string
     * with different values. However, the key appears only once in
     * the hashtable, with its value being
     * an array of strings containing the multiple values sent
     * by the query string.
     * <p>
     * <p>The keys and values in the hashtable are stored in their
     * decoded form, so
     * any + characters are converted to spaces, and characters
     * sent in hexadecimal notation (like <i>%xx</i>) are
     * converted to ASCII characters.
     *
     * @param s a string containing the query to be parsed
     * @throws IllegalArgumentException if the query string is invalid
     * @return a <code>HashTable</code> object built
     * from the parsed key-value pairs
     */
    public static Hashtable<String, String[]> parseQueryString(String s) {

        String valArray[] = null;

        if (s == null) {
            return new Hashtable<>();
        }

        Hashtable<String, String[]> ht = new Hashtable<String, String[]>();
        StringBuilder sb = new StringBuilder();
        StringTokenizer st = new StringTokenizer(s, "&");
        while (st.hasMoreTokens()) {
            String pair = st.nextToken();
            int pos = pair.indexOf('=');
            if (pos == -1) {
                // XXX
                // should give more detail about the illegal argument
                throw new IllegalArgumentException();
            }
            String key = parseName(pair.substring(0, pos), sb);
            String val = parseName(pair.substring(pos + 1, pair.length()), sb);
            if (ht.containsKey(key)) {
                String oldVals[] = ht.get(key);
                valArray = new String[oldVals.length + 1];
                for (int i = 0; i < oldVals.length; i++) {
                    valArray[i] = oldVals[i];
                }
                valArray[oldVals.length] = val;
            } else {
                valArray = new String[1];
                valArray[0] = val;
            }
            ht.put(key, valArray);
        }

        return ht;
    }

    /*
         * Parse a name in the query string.
         */
    private static String parseName(String s, StringBuilder sb) {
        sb.setLength(0);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '+':
                    sb.append(' ');
                    break;
                case '%':
                    try {
                        sb.append((char) Integer.parseInt(s.substring(i + 1, i + 3),
                                16));
                        i += 2;
                    } catch (NumberFormatException e) {
                        // XXX
                        // need to be more specific about illegal arg
                        throw new IllegalArgumentException();
                    } catch (StringIndexOutOfBoundsException e) {
                        String rest = s.substring(i);
                        sb.append(rest);
                        if (rest.length() == 2)
                            i++;
                    }

                    break;
                default:
                    sb.append(c);
                    break;
            }
        }
        return sb.toString();
    }

    public Resource.Builder getRootResource() {
        return rootResource;
    }


    public AbstractController(String root, DataSource dataSource, ObjectMapper objectMapper) throws SQLException, NoSuchMethodException {
        this.root = root;
        this.rootResource = Resource.builder();
        this.rootResource.path(this.root);
        this.dataSource = dataSource;
        this.objectMapper = objectMapper;
        objectMapper.addMixIn(BasicDynaBean.class, SchemaController.DynaJsonMixin.class);
        init();
    }

    protected List getList(String sql) throws SQLException {
        logger.info("Query for get list " + this.root);
        logger.info(sql);
        Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet rs = statement.executeQuery();
        RowSetDynaClass rsdc = new RowSetDynaClass(rs);
        rs.close();
        statement.close();
        connection.close();
        return (List) rsdc.getRows().stream().map(new Function() {
            @Override
            public Object apply(Object o) {
                return ((BasicDynaBean) o).getMap();
            }
        }).collect(Collectors.toList());
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
        logger.info(String.format((String) arg, rem));
    }

    protected List<String> getSingleColumnFromResultSet(ResultSet rs, Integer colId) throws SQLException {
        List<String> result = new LinkedList<>();
        while (rs.next()) {
            result.add(rs.getString(colId));
        }
        return result;
    }

    protected List<String> getSingleColumnFromResultSet(ResultSet rs, String colName) throws SQLException {
        List<String> result = new LinkedList<>();
        while (rs.next()) {
            result.add(rs.getString(colName));
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

    public abstract class DynaJsonMixin {
        @JsonIgnore
        public DynaClass dynaClass;

    }
}
