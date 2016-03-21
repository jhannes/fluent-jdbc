package org.fluentjdbc.demo;

import org.fluentjdbc.Row;
import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.ToString;

@ToString
public class EntryAggregate {

    @Getter
    private Entry entry;

    @Getter
    private List<Tag> tags = new ArrayList<>();

    public EntryAggregate(Entry entry) {
        this.entry = entry;
    }

    public static EntryAggregate retrieve(Connection connection, Long id) {
        EntryAggregate result = new EntryAggregate(Entry.retrieve(connection, id));
        result.getTags().addAll(Tag.tagsTable
            .whereExpression("id in (select tag_id from entry_taggings where entry_id = ?)", id)
            .list(connection, Tag::mapFromRow));
        return result;
    }

    public static List<EntryAggregate> list(Connection connection) {

        String sql = "select * from entries e inner join entry_taggings et on e.id = et.entry_id inner join tags t on et.tag_id = t.id order by e.id";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            try (ResultSet rs = stmt.executeQuery()) {
                Row entryColumns = filterOnTable(rs, "entries");
                Row tagColumns = filterOnTable(rs, "tags");

                ArrayList<EntryAggregate> result = new ArrayList<>();
                EntryAggregate aggregate = null;
                while (rs.next()) {
                    if (aggregate == null || aggregate.getEntry().getId() != rs.getLong("id")) {
                        aggregate = new EntryAggregate(Entry.mapFromRow(entryColumns));
                        result.add(aggregate);
                    }

                    aggregate.getTags().add(Tag.mapFromRow(tagColumns));
                }

                return result;
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    private static Row filterOnTable(ResultSet rs, String tableName) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        Map<String, Integer> columnIndexes = new HashMap<>();
        for (int i=1; i<=metaData.getColumnCount(); i++) {
            if (metaData.getTableName(i).equalsIgnoreCase(tableName)) {
                columnIndexes.put(metaData.getColumnName(i), i);
            }
        }

        return new Row(rs, tableName) {
            @Override
            public Long getLong(String fieldName) throws SQLException {
                return rs.getLong(getColumnIndex(fieldName));
            }

            public Integer getColumnIndex(String fieldName) {
                if (!columnIndexes.containsKey(fieldName.toUpperCase())) {
                    throw new IllegalArgumentException("Column {" + fieldName + "} is not present in {" + tableName + "}: " + columnIndexes.keySet());
                }
                return columnIndexes.get(fieldName.toUpperCase());
            }

            @Override
            public String getString(String fieldName) throws SQLException {
                return rs.getString(getColumnIndex(fieldName));
            }
        };
    }

}
