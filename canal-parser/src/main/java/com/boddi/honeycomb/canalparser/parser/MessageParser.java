/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.boddi.honeycomb.canalparser.parser;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.boddi.honeycomb.canalparser.exception.SelectException;
import com.boddi.honeycomb.canalparser.model.EventColumn;
import com.boddi.honeycomb.canalparser.model.EventColumnIndexComparable;
import com.boddi.honeycomb.canalparser.model.EventData;
import com.boddi.honeycomb.canalparser.model.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.*;


public class MessageParser {

    private static final Logger logger = LoggerFactory.getLogger(MessageParser.class);

    /**
     * 将对应canal送出来的Entry对象解析为otter使用的内部对象
     * 
     * <pre>
     * 需要处理数据过滤：
     * 1. Transaction Begin/End过滤
     * 2. retl.retl_client/retl.retl_mark 回环标记处理以及后续的回环数据过滤
     * 3. retl.xdual canal心跳表数据过滤
     * </pre>
     */
    public List<EventData> parse(List<Entry> datas) {
        List<EventData> eventDatas = new ArrayList<EventData>();
        List<Entry> transactionDataBuffer = new ArrayList<Entry>();
        boolean isLoopback = false;
        boolean needLoopback = false; // 判断是否属于需要loopback处理的类型，只处理正常otter同步产生的回环数据，因为会有业务方手工屏蔽同步的接口，避免回环

        long now = new Date().getTime();
        try {
            for (Entry entry : datas) {
                switch (entry.getEntryType()) {
                    case TRANSACTIONBEGIN:
                        isLoopback = false;
                        break;
                    case ROWDATA:
                        if (!isLoopback) {
                            transactionDataBuffer.add(entry);
                        }
                        break;
                    case TRANSACTIONEND:
                        // 添加数据解析
                        for (Entry bufferEntry : transactionDataBuffer) {
                            List<EventData> parseDatas = internParse(bufferEntry);
                            if (CollectionUtils.isEmpty(parseDatas)) {// 可能为空，针对ddl返回时就为null
                                continue;
                            }

                            // 初步计算一下事件大小
                            long totalSize = bufferEntry.getHeader().getEventLength();
                            long eachSize = totalSize / parseDatas.size();
                            for (EventData eventData : parseDatas) {
                                if (eventData == null) {
                                    continue;
                                }

                                eventData.setSize(eachSize);// 记录一下大小
                                if (needLoopback) {// 针对需要回环同步的
                                    // 如果延迟超过指定的阀值，则设置为需要反查db
                                    if (now - eventData.getExecuteTime() > 1000 * 100) {
                                        eventData.setSyncConsistency(EventData.SyncConsistency.MEDIA);
                                    } else {
                                        eventData.setSyncConsistency(EventData.SyncConsistency.BASE);
                                    }
                                    eventData.setRemedy(true);
                                }
                                eventDatas.add(eventData);
                            }
                        }

                        isLoopback = false;
                        needLoopback = false;
                        transactionDataBuffer.clear();
                        break;
                    default:
                        break;
                }
            }

            // 添加最后一次的数据，可能没有TRANSACTIONEND
            if (!isLoopback) {
                // 添加数据解析
                for (Entry bufferEntry : transactionDataBuffer) {
                    List<EventData> parseDatas = internParse(bufferEntry);
                    if (CollectionUtils.isEmpty(parseDatas)) {// 可能为空，针对ddl返回时就为null
                        continue;
                    }

                    // 初步计算一下事件大小
                    long totalSize = bufferEntry.getHeader().getEventLength();
                    long eachSize = totalSize / parseDatas.size();
                    for (EventData eventData : parseDatas) {
                        if (eventData == null) {
                            continue;
                        }

                        eventData.setSize(eachSize);// 记录一下大小
                        if (needLoopback) {// 针对需要回环同步的
                            // 如果延迟超过指定的阀值，则设置为需要反查db
                            if (now - eventData.getExecuteTime() > 1000 * 100) {
                                eventData.setSyncConsistency(EventData.SyncConsistency.MEDIA);
                            } else {
                                eventData.setSyncConsistency(EventData.SyncConsistency.BASE);
                            }
                        }
                        eventDatas.add(eventData);
                    }
                }
            }
        } catch (Exception e) {
            throw new SelectException(e);
        }

        return eventDatas;
    }


    private List<EventData> internParse( Entry entry) {
        RowChange rowChange = null;
        try {
            rowChange = RowChange.parseFrom(entry.getStoreValue());
        } catch (Exception e) {
            throw new SelectException("parser of canal-event has an error , data:" + entry.toString(), e);
        }

        if (rowChange == null) {
            return null;
        }

        String schemaName = entry.getHeader().getSchemaName();
        String tableName = entry.getHeader().getTableName();
        EventType eventType = EventType.valueOf(rowChange.getEventType().name());

        // 处理下DDL操作
        if (eventType.isQuery()) {
            // 直接忽略query事件
            return null;
        }


        if (eventType.isDdl()) {
            boolean notExistReturnNull = false;
            if (eventType.isRename()) {
                notExistReturnNull = true;
            }
            boolean ddlSync = true;
            if (ddlSync) {
                // 处理下ddl操作
                EventData eventData = new EventData();
                eventData.setSchemaName(schemaName);
                eventData.setTableName(tableName);
                eventData.setEventType(eventType);
                eventData.setExecuteTime(entry.getHeader().getExecuteTime());
                eventData.setSql(rowChange.getSql());
                eventData.setDdlSchemaName(rowChange.getDdlSchemaName());
//                    eventData.setTableId(dataMedia.getId());
                return Arrays.asList(eventData);
            } else {
                return null;
            }
        }


        List<EventData> eventDatas = new ArrayList<EventData>();
        for (RowData rowData : rowChange.getRowDatasList()) {
            EventData eventData = internParse(entry, rowChange, rowData);
            if (eventData != null) {
                eventDatas.add(eventData);
            }
        }

        return eventDatas;
    }

    /**
     * 解析出从canal中获取的Event事件<br>
     * Oracle:有变更的列值. <br>
     * <i>insert:从afterColumns中获取所有的变更数据<br>
     * <i>delete:从beforeColumns中获取所有的变更数据<br>
     * <i>update:在before中存放所有的主键和变化前的非主键值，在after中存放变化后的主键和非主键值,如果是复合主键，只会存放变化的主键<br>
     * Mysql:可以得到所有变更前和变更后的数据.<br>
     * <i>insert:从afterColumns中获取所有的变更数据<br>
     * <i>delete:从beforeColumns中获取所有的变更数据<br>
     * <i>update:在beforeColumns中存放变更前的所有数据,在afterColumns中存放变更后的所有数据<br>
     */
    private EventData internParse(Entry entry, RowChange rowChange, RowData rowData) {
        EventData eventData = new EventData();
        eventData.setTableName(entry.getHeader().getTableName());
        eventData.setSchemaName(entry.getHeader().getSchemaName());
        eventData.setEventType(EventType.valueOf(rowChange.getEventType().name()));
        eventData.setExecuteTime(entry.getHeader().getExecuteTime());
        EventType eventType = eventData.getEventType();

        boolean useTableTransform = true;


        List<Column> beforeColumns = rowData.getBeforeColumnsList();
        List<Column> afterColumns = rowData.getAfterColumnsList();
        String tableName = eventData.getSchemaName() + "." + eventData.getTableName();

        // 判断一下是否需要all columns
        boolean isRowMode = true; // 如果是rowMode模式，所有字段都需要标记为updated
        boolean needAllColumns = isRowMode || true;

        // 变更后的主键
        Map<String, EventColumn> keyColumns = new LinkedHashMap<String, EventColumn>();
        // 变更前的主键
        Map<String, EventColumn> oldKeyColumns = new LinkedHashMap<String, EventColumn>();
        // 有变化的非主键
        Map<String, EventColumn> notKeyColumns = new LinkedHashMap<String, EventColumn>();

        if (eventType.isInsert()) {
            for (Column column : afterColumns) {
                if (isKey(column)) {
                    keyColumns.put(column.getName(), copyEventColumn(column, true));
                } else {
                    // mysql 有效
                    notKeyColumns.put(column.getName(), copyEventColumn(column, true));
                }
            }
        } else if (eventType.isDelete()) {
            for (Column column : beforeColumns) {
                if (isKey(column)) {
                    keyColumns.put(column.getName(), copyEventColumn(column, true));
                } else {
                    // mysql 有效
                    notKeyColumns.put(column.getName(), copyEventColumn(column, true));
                }
            }
        } else if (eventType.isUpdate()) {
            // 获取变更前的主键.
            for (Column column : beforeColumns) {
                if (isKey(column)) {
                    oldKeyColumns.put(column.getName(), copyEventColumn(column, true));
                    // 同时记录一下new
                    // key,因为mysql5.6之后出现了minimal模式,after里会没有主键信息,需要在before记录中找
                    keyColumns.put(column.getName(), copyEventColumn(column, true));
                } else {
                    if (needAllColumns && entry.getHeader().getSourceType() == CanalEntry.Type.ORACLE) {
                        // 针对行记录同步时，针对oracle记录一下非主键的字段，因为update时针对未变更的字段在aftercolume里没有
                        notKeyColumns.put(column.getName(), copyEventColumn(column, isRowMode));
                    }
                }
            }
            for (Column column : afterColumns) {
                // 在update操作时，oracle和mysql存放变更的非主键值的方式不同,oracle只有变更的字段;
                // mysql会把变更前和变更后的字段都发出来，只需要取有变更的字段.
                // 如果是oracle库，after里一定为对应的变更字段

                boolean isUpdate = true;
                if (entry.getHeader().getSourceType() == CanalEntry.Type.MYSQL) { // mysql的after里部分数据为未变更,oracle里after里为变更字段
                    isUpdate = column.getUpdated();
                }

                if (isKey(column)) {
                    // 获取变更后的主键
                    keyColumns.put(column.getName(), copyEventColumn(column, isRowMode || isUpdate));
                } else if (needAllColumns || entry.getHeader().getSourceType() == CanalEntry.Type.ORACLE
                           || column.getUpdated()) {
                    notKeyColumns.put(column.getName(), copyEventColumn(column, isRowMode || isUpdate));// 如果是rowMode，所有字段都为updated
                }
            }

            if (entry.getHeader().getSourceType() == CanalEntry.Type.ORACLE) { // 针对oracle进行特殊处理
                checkUpdateKeyColumns(oldKeyColumns, keyColumns);
            }
        }

        List<EventColumn> keys = new ArrayList<EventColumn>(keyColumns.values());
        List<EventColumn> oldKeys = new ArrayList<EventColumn>(oldKeyColumns.values());
        List<EventColumn> columns = new ArrayList<EventColumn>(notKeyColumns.values());

        Collections.sort(keys, new EventColumnIndexComparable());
        Collections.sort(oldKeys, new EventColumnIndexComparable());
        Collections.sort(columns, new EventColumnIndexComparable());
        if (!keyColumns.isEmpty()) {
            eventData.setKeys(keys);
            if (eventData.getEventType().isUpdate() && !oldKeys.equals(keys)) { // update类型，如果存在主键不同,则记录下old
                                                                                // keys为变更前的主键
                eventData.setOldKeys(oldKeys);
            }
            eventData.setColumns(columns);
            // } else if (CanalEntry.Type.MYSQL ==
            // entry.getHeader().getSourceType()) {
            // // 只支持mysql无主键同步
            // if (eventType.isUpdate()) {
            // List<EventColumn> oldColumns = new ArrayList<EventColumn>();
            // List<EventColumn> newColumns = new ArrayList<EventColumn>();
            // for (Column column : beforeColumns) {
            // oldColumns.add(copyEventColumn(column, true, tableHolder));
            // }
            //
            // for (Column column : afterColumns) {
            // newColumns.add(copyEventColumn(column, true, tableHolder));
            // }
            // Collections.sort(oldColumns, new EventColumnIndexComparable());
            // Collections.sort(newColumns, new EventColumnIndexComparable());
            // eventData.setOldKeys(oldColumns);// 做为老主键
            // eventData.setKeys(newColumns);// 做为新主键，需要保证新老主键字段数量一致
            // } else {
            // // 针对无主键，等同为所有都是主键进行处理
            // eventData.setKeys(columns);
            // }
        } else {
            throw new SelectException("this rowdata has no pks , entry: " + entry.toString() + " and rowData: "
                                      + rowData);
        }

        return eventData;
    }

    /**
     * 在oracle中，补充没有变更的主键<br>
     * 如果变更后的主键为空，直接从old中拷贝<br>
     * 如果变更前后的主键数目不相等，把old中存在而new中不存在的主键拷贝到new中.
     * 
     */
    private void checkUpdateKeyColumns(Map<String, EventColumn> oldKeyColumns, Map<String, EventColumn> keyColumns) {
        // 在变更前没有主键的情况
        if (oldKeyColumns.size() == 0) {
            return;
        }
        // 变更后的主键数据大于变更前的，不符合
        if (keyColumns.size() > oldKeyColumns.size()) {
            return;
        }
        // 主键没有变更，把所有变更前的主键拷贝到变更后的主键中.
        if (keyColumns.size() == 0) {
            keyColumns.putAll(oldKeyColumns);
            return;
        }

        // 把old中存在而new中不存在的主键拷贝到new中
        if (oldKeyColumns.size() != keyColumns.size()) {
            for (String oldKey : oldKeyColumns.keySet()) {
                if (keyColumns.get(oldKey) == null) {
                    keyColumns.put(oldKey, oldKeyColumns.get(oldKey));
                }
            }
        }
    }

    /**
     * 把 erosa-protocol's Column 转化成 otter's model EventColumn.
     *
     * @param column
     * @return
     */
    private EventColumn copyEventColumn(Column column, boolean isUpdate) {
        EventColumn eventColumn = new EventColumn();
        eventColumn.setIndex(column.getIndex());
        eventColumn.setKey(column.getIsKey());
        eventColumn.setNull(column.getIsNull());
        eventColumn.setColumnName(column.getName());
        eventColumn.setColumnValue(column.getValue());
        eventColumn.setUpdate(isUpdate);
        eventColumn.setColumnType(column.getSqlType());

        return eventColumn;
    }

    private boolean isKey(Column column) {
        boolean isEKey = column.getIsKey();
        return isEKey;
    }


}
