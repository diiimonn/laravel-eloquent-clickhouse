<?php

declare(strict_types=1);

namespace One23\LaravelClickhouse\Database\Schema\Grammars;

use Illuminate\Support\Fluent;
use One23\LaravelClickhouse\Database\Connection;
use One23\LaravelClickhouse\Database\Schema\Blueprint;
use One23\LaravelClickhouse\Database\Schema\ColumnDefinition;

class ClickhouseGrammar
{
    /**
     * Compile a create table command.
     */
    public function compileCreate(Blueprint $blueprint, Fluent $command, Connection $connection): string
    {
        $columns = implode(', ', $this->getColumns($blueprint));

        $sql = "CREATE TABLE {$this->wrapTable($blueprint)}";

        if ($cluster = $blueprint->getOnCluster()) {
            $sql .= " ON CLUSTER {$cluster}";
        }

        $sql .= " ({$columns})";

        $sql .= $this->compileEngine($blueprint);
        $sql .= $this->compileOrderBy($blueprint);
        $sql .= $this->compilePartitionBy($blueprint);
        $sql .= $this->compilePrimaryKey($blueprint);
        $sql .= $this->compileTtl($blueprint);
        $sql .= $this->compileSettings($blueprint);

        return $sql;
    }

    /**
     * Compile a drop table command.
     */
    public function compileDrop(Blueprint $blueprint, Fluent $command, Connection $connection): string
    {
        $sql = "DROP TABLE {$this->wrapTable($blueprint)}";

        if ($cluster = $blueprint->getOnCluster()) {
            $sql .= " ON CLUSTER {$cluster}";
        }

        return $sql;
    }

    /**
     * Compile a drop table if exists command.
     */
    public function compileDropIfExists(Blueprint $blueprint, Fluent $command, Connection $connection): string
    {
        $sql = "DROP TABLE IF EXISTS {$this->wrapTable($blueprint)}";

        if ($cluster = $blueprint->getOnCluster()) {
            $sql .= " ON CLUSTER {$cluster}";
        }

        return $sql;
    }

    /**
     * Compile a rename table command.
     */
    public function compileRename(Blueprint $blueprint, Fluent $command, Connection $connection): string
    {
        $from = $this->wrapTable($blueprint);
        $to = $this->wrap($command->to);

        $sql = "RENAME TABLE {$from} TO {$to}";

        if ($cluster = $blueprint->getOnCluster()) {
            $sql .= " ON CLUSTER {$cluster}";
        }

        return $sql;
    }

    /**
     * Compile an add column command.
     */
    public function compileAdd(Blueprint $blueprint, Fluent $command, Connection $connection): array
    {
        $statements = [];
        $table = $this->wrapTable($blueprint);

        foreach ($blueprint->getAddedColumns() as $column) {
            $sql = "ALTER TABLE {$table}";

            if ($cluster = $blueprint->getOnCluster()) {
                $sql .= " ON CLUSTER {$cluster}";
            }

            $sql .= ' ADD COLUMN ' . $this->compileColumn($column);

            if (isset($column->after)) {
                $sql .= ' AFTER ' . $this->wrap($column->after);
            } elseif (isset($column->first) && $column->first) {
                $sql .= ' FIRST';
            }

            $statements[] = $sql;
        }

        return $statements;
    }

    /**
     * Compile a drop column command.
     */
    public function compileDropColumn(Blueprint $blueprint, Fluent $command, Connection $connection): array
    {
        $statements = [];
        $table = $this->wrapTable($blueprint);

        foreach ($command->columns as $column) {
            $sql = "ALTER TABLE {$table}";

            if ($cluster = $blueprint->getOnCluster()) {
                $sql .= " ON CLUSTER {$cluster}";
            }

            $sql .= ' DROP COLUMN ' . $this->wrap($column);

            $statements[] = $sql;
        }

        return $statements;
    }

    /**
     * Compile a rename column command.
     */
    public function compileRenameColumn(Blueprint $blueprint, Fluent $command, Connection $connection): string
    {
        $table = $this->wrapTable($blueprint);

        $sql = "ALTER TABLE {$table}";

        if ($cluster = $blueprint->getOnCluster()) {
            $sql .= " ON CLUSTER {$cluster}";
        }

        $sql .= ' RENAME COLUMN ' . $this->wrap($command->from) . ' TO ' . $this->wrap($command->to);

        return $sql;
    }

    /**
     * Compile a modify column command.
     */
    public function compileModifyColumn(Blueprint $blueprint, Fluent $command, Connection $connection): string
    {
        $table = $this->wrapTable($blueprint);

        $sql = "ALTER TABLE {$table}";

        if ($cluster = $blueprint->getOnCluster()) {
            $sql .= " ON CLUSTER {$cluster}";
        }

        $sql .= ' MODIFY COLUMN ' . $this->wrap($command->column) . ' ' . $command->type;

        return $sql;
    }

    /**
     * Compile an add index command.
     */
    public function compileAddIndex(Blueprint $blueprint, Fluent $command, Connection $connection): string
    {
        $table = $this->wrapTable($blueprint);
        $columns = implode(', ', array_map([$this, 'wrap'], $command->columns));

        $sql = "ALTER TABLE {$table}";

        if ($cluster = $blueprint->getOnCluster()) {
            $sql .= " ON CLUSTER {$cluster}";
        }

        $sql .= " ADD INDEX {$command->name} ({$columns}) TYPE {$command->type} GRANULARITY {$command->granularity}";

        return $sql;
    }

    /**
     * Compile a drop index command.
     */
    public function compileDropIndex(Blueprint $blueprint, Fluent $command, Connection $connection): string
    {
        $table = $this->wrapTable($blueprint);

        $sql = "ALTER TABLE {$table}";

        if ($cluster = $blueprint->getOnCluster()) {
            $sql .= " ON CLUSTER {$cluster}";
        }

        $sql .= " DROP INDEX {$command->name}";

        return $sql;
    }

    /**
     * Get the column definitions for a table.
     */
    protected function getColumns(Blueprint $blueprint): array
    {
        $columns = [];

        foreach ($blueprint->getColumns() as $column) {
            $columns[] = $this->compileColumn($column);
        }

        return $columns;
    }

    /**
     * Compile a column definition.
     */
    protected function compileColumn(ColumnDefinition $column): string
    {
        $type = $this->getType($column);

        if (isset($column->nullable) && $column->nullable) {
            $sql = $this->wrap($column->name) . ' Nullable(' . $type . ')';
        } else {
            $sql = $this->wrap($column->name) . ' ' . $type;
        }

        if (isset($column->default)) {
            $sql .= ' DEFAULT ' . $this->getDefaultValue($column->default);
        }

        if (isset($column->defaultExpression)) {
            $sql .= ' DEFAULT ' . $column->defaultExpression;
        }

        if (isset($column->materialized)) {
            $sql .= ' MATERIALIZED ' . $column->materialized;
        }

        if (isset($column->alias)) {
            $sql .= ' ALIAS ' . $column->alias;
        }

        if (isset($column->codec)) {
            $sql .= ' CODEC(' . $column->codec . ')';
        }

        if (isset($column->ttl)) {
            $sql .= ' TTL ' . $column->ttl;
        }

        if (isset($column->comment)) {
            $sql .= " COMMENT '" . addslashes($column->comment) . "'";
        }

        return $sql;
    }

    /**
     * Get the SQL type for a column.
     */
    protected function getType(ColumnDefinition $column): string
    {
        $type = $column->type;

        return match ($type) {
            'FixedString' => "FixedString({$column->length})",
            'Decimal' => "Decimal({$column->precision}, {$column->scale})",
            'Decimal32', 'Decimal64', 'Decimal128', 'Decimal256' => "{$type}({$column->scale})",
            'DateTime' => $column->timezone ? "DateTime('{$column->timezone}')" : 'DateTime',
            'DateTime64' => $this->compileDatetime64($column),
            'Enum8', 'Enum16' => $this->compileEnum($type, $column->values),
            'Array' => "Array({$column->innerType})",
            'Tuple' => 'Tuple(' . implode(', ', $column->types) . ')',
            'Map' => "Map({$column->keyType}, {$column->valueType})",
            'LowCardinality' => "LowCardinality({$column->innerType})",
            'Nested' => $this->compileNested($column),
            default => $type,
        };
    }

    protected function compileDatetime64(ColumnDefinition $column): string
    {
        $precision = $column->precision ?? 3;

        return $column->timezone
            ? "DateTime64({$precision}, '{$column->timezone}')"
            : "DateTime64({$precision})";
    }

    protected function compileEnum(string $type, array $values): string
    {
        $pairs = [];
        foreach ($values as $key => $value) {
            if (is_int($key)) {
                $pairs[] = "'{$value}' = {$key}";
            } else {
                $pairs[] = "'{$key}' = {$value}";
            }
        }

        return "{$type}(" . implode(', ', $pairs) . ')';
    }

    protected function compileNested(ColumnDefinition $column): string
    {
        $nestedColumns = [];
        foreach ($column->nested->getColumns() as $nestedColumn) {
            $nestedColumns[] = $this->wrap($nestedColumn->name) . ' ' . $this->getType($nestedColumn);
        }

        return 'Nested(' . implode(', ', $nestedColumns) . ')';
    }

    /**
     * Get the default value for a column.
     */
    protected function getDefaultValue(mixed $value): string
    {
        if (is_null($value)) {
            return 'NULL';
        }

        if (is_bool($value)) {
            return $value ? '1' : '0';
        }

        if (is_numeric($value)) {
            return (string) $value;
        }

        if (is_array($value)) {
            return '[' . implode(', ', array_map([$this, 'getDefaultValue'], $value)) . ']';
        }

        return "'" . addslashes((string) $value) . "'";
    }

    /**
     * Compile the engine clause.
     */
    protected function compileEngine(Blueprint $blueprint): string
    {
        $engine = $blueprint->getEngine() ?? 'MergeTree';
        $params = $blueprint->getEngineParameters();

        if (empty($params)) {
            return " ENGINE = {$engine}()";
        }

        $paramString = implode(', ', array_map(function ($param) {
            return is_string($param) ? $param : (string) $param;
        }, $params));

        return " ENGINE = {$engine}({$paramString})";
    }

    /**
     * Compile the ORDER BY clause.
     */
    protected function compileOrderBy(Blueprint $blueprint): string
    {
        $orderBy = $blueprint->getOrderBy();

        if (empty($orderBy)) {
            return '';
        }

        $columns = implode(', ', array_map([$this, 'wrap'], $orderBy));

        return " ORDER BY ({$columns})";
    }

    /**
     * Compile the PARTITION BY clause.
     */
    protected function compilePartitionBy(Blueprint $blueprint): string
    {
        $partitionBy = $blueprint->getPartitionBy();

        if (empty($partitionBy)) {
            return '';
        }

        return ' PARTITION BY ' . implode(', ', $partitionBy);
    }

    /**
     * Compile the PRIMARY KEY clause.
     */
    protected function compilePrimaryKey(Blueprint $blueprint): string
    {
        $primaryKey = $blueprint->getPrimaryKey();

        if (empty($primaryKey)) {
            return '';
        }

        return ' PRIMARY KEY ' . $this->wrap($primaryKey);
    }

    /**
     * Compile the TTL clause.
     */
    protected function compileTtl(Blueprint $blueprint): string
    {
        $ttl = $blueprint->getTtl();
        $column = $blueprint->getTtlColumn();

        if (empty($ttl) || empty($column)) {
            return '';
        }

        return " TTL {$column} + INTERVAL {$ttl} SECOND";
    }

    /**
     * Compile the SETTINGS clause.
     */
    protected function compileSettings(Blueprint $blueprint): string
    {
        $settings = $blueprint->getSettings();

        if (empty($settings)) {
            return '';
        }

        $settingsStr = [];
        foreach ($settings as $key => $value) {
            if (is_bool($value)) {
                $value = $value ? '1' : '0';
            } elseif (is_string($value)) {
                $value = "'{$value}'";
            }
            $settingsStr[] = "{$key} = {$value}";
        }

        return ' SETTINGS ' . implode(', ', $settingsStr);
    }

    /**
     * Wrap a table in keyword identifiers.
     */
    public function wrapTable(Blueprint $blueprint): string
    {
        return $this->wrap($blueprint->getTable());
    }

    /**
     * Wrap a value in keyword identifiers.
     */
    public function wrap(string $value): string
    {
        if (str_contains($value, '.')) {
            return implode('.', array_map([$this, 'wrapValue'], explode('.', $value)));
        }

        return $this->wrapValue($value);
    }

    /**
     * Wrap a single string in backticks.
     */
    protected function wrapValue(string $value): string
    {
        if ($value === '*') {
            return $value;
        }

        return '`' . str_replace('`', '``', $value) . '`';
    }
}
