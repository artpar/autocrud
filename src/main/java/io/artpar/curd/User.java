package io.artpar.curd;

import java.util.List;

/**
 * Created by parth on 30/4/16.
 */
public class User implements UserInterface {
    Long id;
    List<Long> userGroupId;

    @Override
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public List<Long> getUserGroupId() {
        return userGroupId;
    }

    public void setUserGroupId(List<Long> userGroupId) {
        this.userGroupId = userGroupId;
    }
}
