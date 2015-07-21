package com.despegar.p13n.migrator;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface Consumer {

    public void consume(List<Map<String, Object>> keyvalues);

    public void flush() throws IOException;

}
