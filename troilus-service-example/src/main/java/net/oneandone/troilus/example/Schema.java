package net.oneandone.troilus.example;


import java.io.IOException;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;






public class Schema {

    public static final String load(String name) {
        try {
            return Resources.toString(Resources.getResource(name), Charsets.US_ASCII);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

}


