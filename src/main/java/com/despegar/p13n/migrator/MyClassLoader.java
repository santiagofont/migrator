package com.despegar.p13n.migrator;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

public class MyClassLoader
    extends URLClassLoader {

    private ClassLoader exceptionClassLoader;

    private List<String> names;


    public MyClassLoader(URL[] urls, ClassLoader exceptionClassLoader, Class<?>... exceptions) {
        super(urls, null);
        this.exceptionClassLoader = exceptionClassLoader;
        this.names = new ArrayList<>();
        for (Class<?> item : exceptions) {
            this.names.add(item.getName());
        }
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        if (this.names.contains(name)) {
            return this.exceptionClassLoader.loadClass(name);
        } else {
            return super.loadClass(name);
        }
    }

    @Override
    public String toString() {
        return "MyClassLoader , with parent:" + this.getParent();
    }

}
