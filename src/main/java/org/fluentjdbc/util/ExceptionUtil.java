package org.fluentjdbc.util;

public class ExceptionUtil {

    public static RuntimeException softenCheckedException(Exception e) {
        return softenHelper(e);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Exception> T softenHelper(Exception e) throws T {
        throw (T)e;
    }

}
