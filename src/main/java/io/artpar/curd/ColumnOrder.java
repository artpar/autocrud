package io.artpar.curd;

/**
 * Created by parth on 30/4/16.
 */
public class ColumnOrder {
    String name;
    ColumnDirection dir = ColumnDirection.ASC;

    public ColumnOrder(String name, ColumnDirection dir) {
        this.name = name;
        this.dir = dir;
    }

    public ColumnOrder(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ColumnDirection getDir() {
        return dir;
    }

    public void setDir(ColumnDirection dir) {
        this.dir = dir;
    }

    @Override
    public String toString() {
        return name + " " + dir;
    }
}
