package io.artpar.curd;

import java.util.List;

/**
 * Created by parth on 30/4/16.
 */
public class User {
    Integer id;
    List<Integer> userGroupId;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public List<Integer> getUserGroupId() {
        return userGroupId;
    }

    public void setUserGroupId(List<Integer> userGroupId) {
        this.userGroupId = userGroupId;
    }
}
