package io.artpar.curd;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.beanutils.BasicDynaBean;
import org.apache.commons.beanutils.RowSetDynaClass;
import org.glassfish.jersey.process.Inflector;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.model.Resource;

import javax.sql.DataSource;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by parth on 30/4/16.
 */
public class ListController extends AbstractController implements Inflector<ContainerRequestContext, Object> {


    private String tableName;
    private TableData tableData;

    public ListController(String tableName, TableData tableData, DataSource dataSource, ObjectMapper objectMapper) throws SQLException, NoSuchMethodException {
        super(tableName, dataSource, objectMapper);
        this.tableName = tableName;
        this.tableData = tableData;

        this.rootResource.addMethod("GET").produces(MediaType.APPLICATION_JSON_TYPE).handledBy(this, ListController.class.getMethod("apply", ContainerRequestContext.class));

    }

    public Object apply(ContainerRequestContext containerRequestContext) {
        Collection<String> propertyNames = containerRequestContext.getPropertyNames();
        String query = ((ContainerRequest) containerRequestContext).getRequestUri().getQuery();

        try {
            if (tableData.getColumnList() == null) {
                Connection connection = this.dataSource.getConnection();
                DatabaseMetaData databaseMetaData = connection.getMetaData();
                connection.close();
                ResultSet columns = databaseMetaData.getColumns(null, null, tableName, null);
                debugColumnNames(columns);
                List<String> columnNames = getSingleColumnFromResultSet(columns, "COLUMN_NAME");
                columns.close();
                tableData.setColumnList(columnNames);
            }



            List<String> columnNames = tableData.getColumnList();
            List<String> finalList = new LinkedList<>();
            Hashtable<String, String[]> queryParams = parseQueryString(query);
            String[] columnFilterList = queryParams.get("column");
            if (columnFilterList != null && columnFilterList[0].length() > 0) {
                String[] columnInRequest = queryParams.get("column");
                List<String> names = Arrays.asList( columnInRequest[0].split(",") );
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
            List<String> finalOrders = new LinkedList<>();
            if (order != null && order.length >0 && order[0].length() > 0) {
                String[] orders = order[0].split(",");
                for (String s : orders) {
                    if (tableData.getColumnList().contains(s)) {
                        finalOrders.add(s);
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
                }catch (NumberFormatException e) {

                }
            }

            if (offset != null && offset.length > 0 && offset[0].length() > 0) {
                try {
                    actualOffset = Integer.valueOf(offset[0]);
                }catch (NumberFormatException e) {

                }
            }


            String columns = String.join(",", columnNames);
            return paginatedResult("select ", columns, " from " + tableName, finalOrders, actualOffset, actualLimit );
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
    protected void init() {

    }
}
