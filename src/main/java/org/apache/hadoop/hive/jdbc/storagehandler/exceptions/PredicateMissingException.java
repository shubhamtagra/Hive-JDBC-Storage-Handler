package org.apache.hadoop.hive.jdbc.storagehandler.exceptions;

import java.io.IOException;

/**
 * Created by stagra on 7/23/15.
 */
public class PredicateMissingException extends RuntimeException
{
    public PredicateMissingException() {
        super("No conditions available for pushdown");
    }
}
