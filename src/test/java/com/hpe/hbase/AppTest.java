package com.hpe.hbase;

import static org.junit.Assert.assertTrue;
import java.util.*;

import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }

    public static void main(String[] args) {
/*
        HbaseUtils hu = new HbaseUtils();
        for ( String str : hu.listTables())
        {
            System.out.println(str);
        }
        hu.dropTable("tb001");

        hu.createTable("tb001", new String[]{"cf1", "cf2"});

        hu.insertData("tb001", "rk1", "cf1", "a", "001");
        hu.insertData("tb001", "rk1", "cf2", "b", "002");

        List<String[]> ls = new ArrayList<>();
        ls.add(new String[]{"rk2", "cf1", "c", "003"});
        ls.add(new String[]{"rk3", "cf1", "d", "003"});
        ls.add(new String[]{"rk4", "cf1", "e", "003"});
        hu.insertDataBatch("tb001", ls);

        //hu.delData("tb01", "rk1");

        //hu.getData("tb01", "rk2");
        hu.getBatchData("tb001", "rk2", "rk4");

        hu.close();*/

    }
}
