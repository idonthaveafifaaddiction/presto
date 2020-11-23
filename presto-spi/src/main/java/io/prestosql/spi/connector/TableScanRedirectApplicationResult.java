/*
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
package io.prestosql.spi.connector;

import io.prestosql.spi.predicate.TupleDomain;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TableScanRedirectApplicationResult
{
    private final CatalogSchemaTableName destinationTable;
    // mapping of source table handles to destination table columns
    private final Map<ColumnHandle, String> destinationColumns;
    // filter that needs to be applied on top of table scan
    private final TupleDomain<String> filter;
    // should coercions be used when source and redirected column types don't match
    private boolean allowCoercions;

    public TableScanRedirectApplicationResult(
            CatalogSchemaTableName destinationTable,
            Map<ColumnHandle, String> destinationColumns,
            TupleDomain<String> filter)
    {
        this(destinationTable, destinationColumns, filter, false);
    }

    public TableScanRedirectApplicationResult(
            CatalogSchemaTableName destinationTable,
            Map<ColumnHandle, String> destinationColumns,
            TupleDomain<String> filter,
            boolean allowCoercions)
    {
        this.destinationTable = requireNonNull(destinationTable, "destinationTable is null");
        this.destinationColumns = Map.copyOf(requireNonNull(destinationColumns, "destinationColumns is null"));
        this.filter = requireNonNull(filter, "filter is null");
        this.allowCoercions = allowCoercions;
    }

    public CatalogSchemaTableName getDestinationTable()
    {
        return destinationTable;
    }

    public Map<ColumnHandle, String> getDestinationColumns()
    {
        return destinationColumns;
    }

    public TupleDomain<String> getFilter()
    {
        return filter;
    }

    public boolean isAllowCoercions()
    {
        return allowCoercions;
    }
}
