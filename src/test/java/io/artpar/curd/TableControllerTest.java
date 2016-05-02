package io.artpar.curd;

import org.junit.Assert;

/**
 * Created by parth on 30/4/16.
 */
public class TableControllerTest {

    @org.junit.Test
    public void testGetNthDigit() throws Exception {
        Assert.assertEquals( 5, AbstractTableController.getNthDigit(765, 0) );
        Assert.assertEquals( 6, AbstractTableController.getNthDigit(765, 1) );
        Assert.assertEquals( 7, AbstractTableController.getNthDigit(765, 2) );
    }
}