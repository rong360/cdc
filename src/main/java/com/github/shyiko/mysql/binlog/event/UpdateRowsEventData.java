/*
 * Copyright 2013 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog.event;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;



/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class UpdateRowsEventData implements EventData {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private long tableId;
    private BitSet includedColumnsBeforeUpdate;
    private BitSet includedColumns;
    /**
     * @see com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer
     */
    private List<Map.Entry<Serializable[], Serializable[]>> rows;

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public BitSet getIncludedColumnsBeforeUpdate() {
        return includedColumnsBeforeUpdate;
    }

    public void setIncludedColumnsBeforeUpdate(BitSet includedColumnsBeforeUpdate) {
        this.includedColumnsBeforeUpdate = includedColumnsBeforeUpdate;
    }

    public BitSet getIncludedColumns() {
        return includedColumns;
    }

    public void setIncludedColumns(BitSet includedColumns) {
        this.includedColumns = includedColumns;
    }

    public List<Map.Entry<Serializable[], Serializable[]>> getRows() {
        return rows;
    }

    public void setRows(List<Map.Entry<Serializable[], Serializable[]>> rows) {
        this.rows = rows;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("UpdateRowsEventData");
        sb.append("{tableId=").append(tableId);
        sb.append(", includedColumnsBeforeUpdate=").append(includedColumnsBeforeUpdate);
        sb.append(", includedColumns=").append(includedColumns);
        sb.append(", rows=[");
        for (Map.Entry<Serializable[], Serializable[]> row : rows) {
            sb.append("\n").
            	 append("{before=").append(convert(row.getKey())).append("\n").
                    append("after=").append(convert(row.getValue())).append("\n").
               append("},");
        }
        if (!rows.isEmpty()) {
            sb.replace(sb.length() - 1, sb.length(), "\n");
        }
        sb.append("]}");
        return sb.toString();
    }
    
    private String convert(Serializable[] tmp){
    	String ret ="";
    	try {
			String translateValue = "";
			for(int i =0; i <tmp.length;i++){
				Serializable c = tmp[i];
				if(c == null){
					translateValue = null;
				}else if(c instanceof byte[]){
					translateValue = new String((byte[]) c,"UTF-8");
				}else if( c instanceof java.sql.Timestamp){
					translateValue = String.valueOf(((java.sql.Timestamp) c).getTime());
				}else if(c instanceof java.sql.Date){
					translateValue = String.valueOf(((java.sql.Date) c).getTime());
				}else{
					translateValue = String.valueOf(c);
				}
				ret += translateValue +" ";
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch bloc
		}
    	return ret;
    }
}
