package com.dcfaight.kafkawkshp001.utils;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HashDemoTest {

    @Test
    void constructor_canBeInstantiated() {
        assertNotNull(new HashDemo());
    }

    @Test
    void main_printsThreePositiveIntegers() {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        System.setOut(new PrintStream(bytes));
        try {
            HashDemo.main(new String[0]);
        } finally {
            System.setOut(originalOut);
        }

        String[] lines = bytes.toString(StandardCharsets.UTF_8).trim().split("\\R");
        assertEquals(3, lines.length);
        for (String line : lines) {
            assertTrue(Integer.parseInt(line) >= 0);
        }
    }
}
