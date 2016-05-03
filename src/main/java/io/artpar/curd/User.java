package io.artpar.curd;

import java.util.List;

/**
 * Created by parth on 30/4/16.
 */
public class User implements UserInterface {
    String id;
    private List<String> groupIdsOfUser;

    public User(String id, List<String> groupIdsOfUser) {

        this.id = id;
        this.groupIdsOfUser = groupIdsOfUser;
    }

    public User() {

    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getGroupIdsOfUser() {
        return groupIdsOfUser;
    }

    public void setGroupIdsOfUser(List<String> groupIdsOfUser) {
        this.groupIdsOfUser = groupIdsOfUser;
    }
}
