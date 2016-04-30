package io.artpar.test;

import io.artpar.curd.User;

import javax.servlet.*;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by parth on 30/4/16.
 */

public class InsertDummyUserFilter implements ContainerRequestFilter {

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        final User o = new User();
        o.setId(1L);
        o.setUserGroupId(Arrays.asList(1L, 2L));
        requestContext.setProperty("user", o);

    }
}
