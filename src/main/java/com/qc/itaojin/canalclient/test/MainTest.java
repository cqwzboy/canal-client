package com.qc.itaojin.canalclient.test;

import com.qc.itaojin.annotation.HBaseColumn;
import com.qc.itaojin.annotation.HBaseEntity;
import com.qc.itaojin.util.StringUtils;

import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;

public class MainTest {

    public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {

        String pak = "com.qc.itaojin.canalclient.test";
        URL url = MainTest.class.getClassLoader().getResource("");
        String absolutePath = url.getPath();
        String a = pak.replaceAll("\\.", "/");
        String path = StringUtils.contact(absolutePath, a);
        System.out.println(path);

        File[] files = new File(path).listFiles();
        for (File file : files) {
            if(file.isFile()){
                String s = file.getAbsolutePath();
                System.out.println("============== isFile : "+s);
                s = s.substring(absolutePath.length()-1);
                System.out.println(s);
                s = s.replaceAll("\\\\", "\\.");
                s = s.substring(0, s.indexOf(".class"));
                System.out.println(s);
                Class clazz = Class.forName(s);
                if(!clazz.isAnnotationPresent(HBaseEntity.class)){
                    System.out.println("clazz must use annotation 'HBaseEntity'");
                    continue;
                }
                Annotation[] annotations = clazz.getAnnotations();
                for (Annotation annotation : annotations) {
                    System.out.println(annotation.annotationType().getName());
                }

                Field[] fields = clazz.getDeclaredFields();
                for (Field field : fields) {
                    if(field.isAnnotationPresent(HBaseColumn.class)){
                        Annotation[] annotations1 = field.getDeclaredAnnotations();
                        for (Annotation annotation : annotations1) {
                            System.out.println("\t"+field.getName()+" "+annotation.toString());
                            System.out.println("\t"+annotation.annotationType().getName());
                            System.out.println("className: "+HBaseColumn.class.getName());
                            System.out.println("\t"+HBaseColumn.class.getName().equals(annotation.annotationType().getName()));
                            System.out.println("\t"+field.isAnnotationPresent(HBaseColumn.class));
                        }
                    }
                }
            }
        }

    }

}
