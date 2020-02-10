package operators;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import config.GeneralConfig;
import data.ColumnData;
import joining.result.ResultTuple;
import org.eclipse.collections.api.list.primitive.IntList;
import query.ColumnRef;

import java.util.*;

/**
 * Materializes parts of a table, defined by
 * a subset of columns and a subset of rows.
 *
 * @author immanueltrummer
 */
public class Materialize {
    /**
     * Creates a temporary table with given name and copies into it
     * values at given row indices and for given columns in source
     * table.
     *
     * @param sourceRelName name of source table to copy from
     * @param columnNames   names of columns to be copied
     * @param rowList       list of row indices to copy, can be null
     * @param rowBitSet     rows to copy in BitSet representation, can be null
     * @param targetRelName name of target table
     * @param tempResult    whether to create temporary result relation
     * @throws Exception
     */
    public static void execute(String sourceRelName, List<String> columnNames,
                               IntList rowList, BitSet rowBitSet,
                               String targetRelName,
                               boolean tempResult) throws Exception {
        // Generate references to source columns
        List<ColumnRef> sourceColRefs = new ArrayList<ColumnRef>();
        for (String columnName : columnNames) {
            sourceColRefs.add(new ColumnRef(sourceRelName, columnName));
        }
        // Update catalog, inserting materialized table
        TableInfo resultTable = new TableInfo(targetRelName, tempResult);
        CatalogManager.currentDB.addTable(resultTable);
        for (ColumnRef sourceColRef : sourceColRefs) {
            // Add result column to result table, using type of source column
            ColumnInfo sourceCol = CatalogManager.getColumn(sourceColRef);
            ColumnInfo resultCol = new ColumnInfo(sourceColRef.columnName,
                    sourceCol.type, sourceCol.isPrimary,
                    sourceCol.isUnique, sourceCol.isNotNull,
                    sourceCol.isForeign);
            resultTable.addColumn(resultCol);
        }
        // Load source data if necessary
        if (!GeneralConfig.inMemory) {
            for (ColumnRef sourceColRef : sourceColRefs) {
                BufferManager.loadColumn(sourceColRef);
            }
        }
        // Generate column data
        sourceColRefs.parallelStream().forEach(sourceColRef -> {
            // Copy relevant rows into result column
            ColumnData srcData = BufferManager.colToData.get(sourceColRef);
            ColumnData resultData = rowList == null ?
                    srcData.copyRows(rowBitSet) : srcData.copyRows(rowList);
            String columnName = sourceColRef.columnName;
            ColumnRef resultColRef = new ColumnRef(targetRelName, columnName);
            BufferManager.colToData.put(resultColRef, resultData);
        });
        // Update statistics in catalog
        CatalogManager.updateStats(targetRelName);
        // Unload source data if necessary
        if (!GeneralConfig.inMemory) {
            for (ColumnRef sourceColRef : sourceColRefs) {
                BufferManager.unloadColumn(sourceColRef);
            }
        }
    }

    /**
     * Materializes a join relation from given indices
     * for a set of base tables.
     *
     * @param tuples         base table indices representing result tuples
     * @param tableToIdx     maps table names to base table indices
     * @param sourceCols     set of columns to copy
     * @param columnMappings maps source columns, as in query, to DB columns
     * @param targetRelName  nam1e of materialized result relation
     * @throws Exception
     */
    public static void execute(Collection<ResultTuple> tuples,
                               Map<String, Integer> tableToIdx,
                               Collection<ColumnRef> sourceCols,
                               Map<ColumnRef, ColumnRef> columnMappings,
                               String targetRelName) throws Exception {
		/*
		// Extract dimensions
		int nrTables = tableToIdx.size();
		int nrTuples = tuples.size();
		// Collect row indices for each table
		Map<Integer, List<Integer>> idxToRows = new ConcurrentHashMap<>();
		IntStream.range(0, nrTables).parallel().forEach(i-> {
			List<Integer> rows = new ArrayList<>(nrTuples);
			for (ResultTuple tuple : tuples) {
				rows.add(tuple.baseIndices[i]);
			}
			idxToRows.put(i, rows);
		});
		*/
        // Update catalog, insert result table
        TableInfo resultInfo = new TableInfo(targetRelName, true);
        CatalogManager.currentDB.addTable(resultInfo);
        // Add result columns to catalog
        for (ColumnRef srcQueryRef : sourceCols) {
            // Map query column to DB column
            ColumnRef srcDBref = columnMappings.get(srcQueryRef);
            // Extract information on source column
            String srcAlias = srcQueryRef.aliasName;
            String srcColName = srcQueryRef.columnName;
            ColumnInfo srcInfo = CatalogManager.getColumn(srcDBref);
            // Generate target column
            String targetColName = srcAlias + "." + srcColName;
            ColumnInfo targetInfo = new ColumnInfo(targetColName,
                    srcInfo.type, false, false, false, false);
            resultInfo.addColumn(targetInfo);
        }
        // Materialize result columns
        sourceCols.parallelStream().forEach(srcQueryRef -> {
            // Generate target column reference
            String targetCol =
                    srcQueryRef.aliasName + "." + srcQueryRef.columnName;
            ColumnRef targetRef = new ColumnRef(targetRelName, targetCol);
            // Generate target column
            int tableIdx = tableToIdx.get(srcQueryRef.aliasName);
            ColumnRef srcDBref = columnMappings.get(srcQueryRef);
            ColumnData srcData = BufferManager.colToData.get(srcDBref);
            ColumnData targetData = srcData.copyRows(tuples, tableIdx);
            // Insert into buffer pool
            BufferManager.colToData.put(targetRef, targetData);
        });
        // Update statistics in catalog
        CatalogManager.updateStats(targetRelName);
    }
}
