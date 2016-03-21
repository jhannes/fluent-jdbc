package org.fluentjdbc.util;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class ExceptionUtil {

    public static RuntimeException softenCheckedException(Exception e) {
        return softenHelper(e);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Exception> T softenHelper(Exception e) throws T {
        throw (T)e;
    }

}
