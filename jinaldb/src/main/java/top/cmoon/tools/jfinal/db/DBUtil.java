package top.cmoon.tools.jfinal.db;


import com.jfinal.plugin.activerecord.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

/**
 * 批量插入工具类，替换Db.batchSave方法，目的是提高MySql批量插入效率，经测试批量插入20000条数据比jfinal官方实现快16倍
 * <p>
 * <br>
 * 注意：此工具只能在mysql数据库中使用，其他数据库将会抛出SQL语法错误
 *
 * @author Administrator
 */
public class DBUtil {

    /**
     * mysql批量插入高效率实现，将插入语句组装为 insert into table (column1, column2 ..) values
     * (?,?),(?,?),...
     * <p>
     * 用于替代Db.batchSave实现
     *
     * @param recordList model列表
     * @param batchSize  批量插入大小，用于分段插入，防止sql语句过长,mysql不能支持
     * @return 返回影响行数，注意：返回数组只有一个元素
     */
    public static <T extends Model<T>> int[] batchSave(List<T> recordList, int batchSize) {
        if (recordList == null || recordList.size() == 0)
            return new int[0];

        Model<T> model = recordList.get(0);
        Table table = TableMapping.me().getTable(model.getClass());
        String tableName = table.getName();

        return batchSaveComm(tableName, recordList, batchSize, Db.class);
    }

    /**
     * mysql批量插入高效率实现，将插入语句组装为 insert into table (column1, column2 ..) values
     * (?,?),(?,?),...
     * <p>
     * 用于替代Db.batchSave实现
     *
     * @param tableName  表名称
     * @param recordList model列表
     * @param batchSize  批量插入大小，用于分段插入，防止sql语句过长,mysql不能支持
     * @return 返回影响行数，注意：返回数组只有一个元素
     */
    public static int[] batchSave(String tableName, List<Record> recordList, int batchSize) {
        return batchSaveComm(tableName, recordList, batchSize, Db.class);
    }

    public static <T extends Model<T>> int[] batchSave(List<T> recordList, int batchSize, Class<?> dbClass) {
        if (recordList == null || recordList.size() == 0)
            return new int[0];

        Model<T> model = recordList.get(0);
        Table table = TableMapping.me().getTable(model.getClass());
        String tableName = table.getName();

        return batchSaveComm(tableName, recordList, batchSize, dbClass);
    }

    private static int[] batchSaveComm(String tableName, List<? extends Object> recordList, int batchSize,
                                       Class<?> dbClass) {
        if (recordList == null || recordList.size() == 0)
            return new int[0];

        Object model = recordList.get(0);

        StringBuilder sqlHeader = null;
        List<String> columnNameList = new ArrayList<>(); // 记录头部的顺序
        sqlHeader = composeInsertHeader(tableName, model, columnNameList);

        // 组装插入值语句 insert table (`column1`,`column2`, ...) values (?, ?, ...),
        // (?,?...), (?,?,...)...
        int affectedRows = 0;
        StringBuilder insertSql = new StringBuilder(sqlHeader.toString());
        List<Object> params = new ArrayList<>(batchSize * columnNameList.size());
        boolean recordFirst = true;

        Method updateMethod = null;

        try {
            updateMethod = dbClass.getMethod("update", String.class, Object[].class);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException("获取update方法发生错误", e);
        }

        int residueCount = 0; // 剩余未在循环中批量插入数据库的记录数，循环退出后，如果大于0，表示需要再次插入
        for (int i = 0; i < recordList.size(); i++) {
            residueCount++;
            Object record = recordList.get(i);

            if (!recordFirst) {
                insertSql.append(",");
            }
            if (recordFirst)
                recordFirst = false;
            insertSql.append("(");

            boolean firstVal = true;
            for (String colName : columnNameList) {
                if (!firstVal) {
                    insertSql.append(",");
                }
                insertSql.append("?");
                params.add(getVal(colName, record));
                if (firstVal)
                    firstVal = false;
            }
            insertSql.append(")");

            if ((i + 1) % batchSize == 0) { // 分批量插入

                // affectedRows += Db.update(insertSql.toString(),
                // params.toArray());
                try {
                    affectedRows += (Integer) (updateMethod.invoke(null,
                            new Object[]{insertSql.toString(), params.toArray()}));
                } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                    e.printStackTrace();
                    throw new RuntimeException("执行update方法发生错误", e);
                }

                // 初始化组装插入之语句条件
                residueCount = 0;
                recordFirst = true;
                insertSql = new StringBuilder(sqlHeader.toString());
                params = new ArrayList<>(batchSize * columnNameList.size());
            }
        }

        if (residueCount > 0) {

            // 插入剩余的记录
            try {
                affectedRows += (Integer) (updateMethod.invoke(null,
                        new Object[]{insertSql.toString(), params.toArray()}));
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                e.printStackTrace();
                throw new RuntimeException("执行update方法发生错误", e);
            }

            //affectedRows += Db.update(insertSql.toString(), params.toArray());
        }

        return new int[]{affectedRows};
    }

    private static StringBuilder composeInsertHeader(String tableName, Object model, List<String> columnNameList) {

        StringBuilder sqlHeader = new StringBuilder();

        // 组装插入语句头部 insert table () values
        sqlHeader.append("insert into `").append(tableName).append("`(");

        if (model instanceof Record) {

            String[] colums = ((Record) model).getColumnNames();
            boolean colFirst = true;
            for (String colName : colums) {
                if (!colFirst) {
                    sqlHeader.append(", ");
                }
                sqlHeader.append("`").append(colName).append("`");
                columnNameList.add(colName);
                if (colFirst)
                    colFirst = false;
            }
        } else if (model instanceof Model) {

            Model<?> ml = (Model<?>) model;

            Table table = TableMapping.me().getTable(ml.getClass());
            boolean colFirst = true;
            for (Entry<String, Object> e : ml._getAttrsEntrySet()) {
                String colName = e.getKey();
                if (table.hasColumnLabel(colName)) {
                    if (!colFirst) {
                        sqlHeader.append(", ");
                    }
                    sqlHeader.append("`").append(colName).append("`");
                    columnNameList.add(colName);
                    if (colFirst)
                        colFirst = false;
                }
            }
        } else {
            throw new RuntimeException("model类型错误，只能是Record或者Model");
        }

        sqlHeader.append(")");
        sqlHeader.append(" values ");

        return sqlHeader;
    }

    private static Object getVal(String colName, Object record) {

        if (record instanceof Record) {
            return ((Record) record).get(colName);
        } else if (record instanceof Model) {
            return ((Model<?>) record).get(colName);
        } else
            throw new RuntimeException("不支持的类型，只能是Record或者Model");

    }

}
