package com.aurora.flink.connector.writer;

import org.apache.flink.annotation.PublicEvolving;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lj.michale
 * @description pojo操作映射器
 * @date 2021-07-07
 * @param <T>
 */
@PublicEvolving
public class PojoOperationMapper<T> extends AbstractSingleOperationMapper<T> {

    /**
     * pojo字段数组
     */
    private final Field[] fields;

    protected PojoOperationMapper(Class<T> pojoClass, String[] columnNames) { this(pojoClass, columnNames, null); }

    public PojoOperationMapper(Class<T> pojoClass, String[] columnNames, KuduOperation operation) {
        super(columnNames, operation);
        fields = initFields(pojoClass, columnNames);
    }

    /**
     * 获取全部Field集合
     * @param fields
     * @param type
     * @return
     */
    public static List<Field> getAllFields(List<Field> fields, Class<?> type) {
        fields.addAll(Arrays.asList(type.getDeclaredFields()));

        /**
         * 获取父类的fields
         */
        if (type.getSuperclass() != null) {
            getAllFields(fields, type.getSuperclass());
        }

        return fields;
    }

    /**
     *  反射初始化fileds数组
     * @param pojoClass
     * @param columnNames
     * @return
     */
    private Field[] initFields(Class<T> pojoClass, String[] columnNames) {
        // 防止重复
        Map<String, Field> allFields = new HashMap<>();
        // 获取当前pojoClass及父类的filed
        getAllFields(new ArrayList<>(), pojoClass).stream().forEach(f -> {
            if (!allFields.containsKey(f.getName())) {
                allFields.put(f.getName(), f);
            }
        });

        Field[] fields = new Field[columnNames.length];

        // 将allFields中包含的columnNames全部放入Field数组
        for (int i = 0; i < columnNames.length; i++) {
            Field f = allFields.get(columnNames[i]);
            if (f == null) {
                throw new RuntimeException("Cannot find field " + columnNames[i] + ". List of detected fields: " + allFields.keySet());
            }
            // 开启强制访问
            f.setAccessible(true);
            fields[i] = f;
        }

        return fields;
    }

    @Override
    public Object getField(T input, int i) {
        try {
            return fields[i].get(input);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("This is a bug");
        }
    }
}