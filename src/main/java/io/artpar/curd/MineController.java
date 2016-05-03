package io.artpar.curd;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.jersey.server.model.Resource;

import javax.annotation.security.RolesAllowed;
import javax.sql.DataSource;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by parth on 30/4/16.
 */
public class MineController extends AbstractTableController {


    public MineController(String tableName, String context, String relativePath, DataSource dataSource, ObjectMapper objectMapper) throws SQLException, NoSuchMethodException {
        super(tableName, context, relativePath, dataSource, objectMapper);

        final Method router = MineController.class.getMethod("router", ContainerRequestContext.class);

        debug("Added GET " + this.context);
        this.rootResource.addMethod("GET").produces(MediaType.APPLICATION_JSON_TYPE).handledBy(this, router);

    }

    @Override
    @RolesAllowed("ROLE_USER")
    public Object router(ContainerRequestContext containerRequestContext) throws IOException, SQLException {
        final String path = containerRequestContext.getUriInfo().getPath();
        final MyRequest myRequest = new MyRequest(containerRequestContext);
        Object re = permissionCheck(myRequest);
        if (re != null) {
            return re;
        }


        String relative = path.substring(path.indexOf(this.root) + this.root.length()) + "/";

        switch (containerRequestContext.getMethod().toLowerCase() + " " + relative) {
            case "get /":
                return this.listMyItem(myRequest);
        }
        return Response.status(Response.Status.NOT_IMPLEMENTED);
    }


    private Object listMyItem(MyRequest myRequest) {
        MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
        params.putSingle("user_id", String.valueOf(myRequest.getUser().getId()));
        params.putSingle("limit", myRequest.getQueryParam("limit"));
        if (myRequest.getQueryParam("children") != null) {
            params.putSingle("children", myRequest.getQueryParam("children"));
        }
        return getResult(params, myRequest.getUser());
    }

}
