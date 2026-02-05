<?php

declare(strict_types=1);

namespace One23\LaravelClickhouse\Database\Schema;

use Closure;
use Illuminate\Support\Fluent;
use One23\LaravelClickhouse\Database\Connection;
use One23\LaravelClickhouse\Database\Schema\Grammars\ClickhouseGrammar;

class Blueprint
{
    protected string $table;

    protected string $prefix = '';

    /** @var ColumnDefinition[] */
    protected array $columns = [];

    /** @var Fluent[] */
    protected array $commands = [];

    // ClickHouse-specific properties
    protected ?string $engine = null;

    protected array $engineParameters = [];

    protected ?array $orderBy = null;

    protected ?array $partitionBy = null;

    protected ?string $primaryKey = null;

    protected ?int $ttl = null;

    protected ?string $ttlColumn = null;

    protected ?string $onCluster = null;

    protected array $settings = [];

    public function __construct(string $table, ?Closure $callback = null, string $prefix = '')
    {
        $this->table = $table;
        $this->prefix = $prefix;

        if (! is_null($callback)) {
            $callback($this);
        }
    }

    // =========================================================================
    // ClickHouse Engine Methods
    // =========================================================================

    /**
     * Set the storage engine for the table.
     */
    public function engine(string $engine, array $parameters = []): self
    {
        $this->engine = $engine;
        $this->engineParameters = $parameters;

        return $this;
    }

    /**
     * Use the MergeTree engine.
     */
    public function mergeTree(): self
    {
        return $this->engine('MergeTree');
    }

    /**
     * Use the ReplacingMergeTree engine for deduplication.
     */
    public function replacingMergeTree(?string $versionColumn = null, ?string $deletedColumn = null): self
    {
        $params = [];
        if ($versionColumn) {
            $params[] = $versionColumn;
        }
        if ($deletedColumn) {
            $params[] = $deletedColumn;
        }

        return $this->engine('ReplacingMergeTree', $params);
    }

    /**
     * Use the SummingMergeTree engine for automatic sum aggregation.
     */
    public function summingMergeTree(array $columns = []): self
    {
        return $this->engine('SummingMergeTree', $columns);
    }

    /**
     * Use the AggregatingMergeTree engine.
     */
    public function aggregatingMergeTree(): self
    {
        return $this->engine('AggregatingMergeTree');
    }

    /**
     * Use the CollapsingMergeTree engine.
     */
    public function collapsingMergeTree(string $signColumn): self
    {
        return $this->engine('CollapsingMergeTree', [$signColumn]);
    }

    /**
     * Use the VersionedCollapsingMergeTree engine.
     */
    public function versionedCollapsingMergeTree(string $signColumn, string $versionColumn): self
    {
        return $this->engine('VersionedCollapsingMergeTree', [$signColumn, $versionColumn]);
    }

    /**
     * Use the Memory engine (data stored in RAM).
     */
    public function memory(): self
    {
        return $this->engine('Memory');
    }

    /**
     * Use the Log engine.
     */
    public function log(): self
    {
        return $this->engine('Log');
    }

    /**
     * Use the TinyLog engine.
     */
    public function tinyLog(): self
    {
        return $this->engine('TinyLog');
    }

    /**
     * Use the StripeLog engine.
     */
    public function stripeLog(): self
    {
        return $this->engine('StripeLog');
    }

    /**
     * Use the Buffer engine.
     */
    public function buffer(
        string $database,
        string $table,
        int $numLayers,
        int $minTime,
        int $maxTime,
        int $minRows,
        int $maxRows,
        int $minBytes,
        int $maxBytes
    ): self {
        return $this->engine('Buffer', [
            $database, $table, $numLayers,
            $minTime, $maxTime,
            $minRows, $maxRows,
            $minBytes, $maxBytes,
        ]);
    }

    /**
     * Use the Distributed engine.
     */
    public function distributed(string $cluster, string $database, string $table, ?string $shardingKey = null): self
    {
        $params = [$cluster, $database, $table];
        if ($shardingKey) {
            $params[] = $shardingKey;
        }

        return $this->engine('Distributed', $params);
    }

    // =========================================================================
    // ClickHouse Table Options
    // =========================================================================

    /**
     * Set the ORDER BY columns for the table.
     */
    public function orderBy(array|string $columns): self
    {
        $this->orderBy = (array) $columns;

        return $this;
    }

    /**
     * Set the PARTITION BY expression.
     */
    public function partitionBy(array|string $expression): self
    {
        $this->partitionBy = (array) $expression;

        return $this;
    }

    /**
     * Set the PRIMARY KEY column.
     */
    public function primaryKey(string $column): self
    {
        $this->primaryKey = $column;

        return $this;
    }

    /**
     * Set TTL for the table.
     */
    public function ttl(string $column, int $intervalSeconds): self
    {
        $this->ttlColumn = $column;
        $this->ttl = $intervalSeconds;

        return $this;
    }

    /**
     * Set the cluster for distributed operations.
     */
    public function onCluster(string $cluster): self
    {
        $this->onCluster = $cluster;

        return $this;
    }

    /**
     * Add a table setting.
     */
    public function setting(string $key, mixed $value): self
    {
        $this->settings[$key] = $value;

        return $this;
    }

    // =========================================================================
    // Integer Column Types
    // =========================================================================

    public function int8(string $column): ColumnDefinition
    {
        return $this->addColumn('Int8', $column);
    }

    public function int16(string $column): ColumnDefinition
    {
        return $this->addColumn('Int16', $column);
    }

    public function int32(string $column): ColumnDefinition
    {
        return $this->addColumn('Int32', $column);
    }

    public function int64(string $column): ColumnDefinition
    {
        return $this->addColumn('Int64', $column);
    }

    public function int128(string $column): ColumnDefinition
    {
        return $this->addColumn('Int128', $column);
    }

    public function int256(string $column): ColumnDefinition
    {
        return $this->addColumn('Int256', $column);
    }

    public function uInt8(string $column): ColumnDefinition
    {
        return $this->addColumn('UInt8', $column);
    }

    public function uInt16(string $column): ColumnDefinition
    {
        return $this->addColumn('UInt16', $column);
    }

    public function uInt32(string $column): ColumnDefinition
    {
        return $this->addColumn('UInt32', $column);
    }

    public function uInt64(string $column): ColumnDefinition
    {
        return $this->addColumn('UInt64', $column);
    }

    public function uInt128(string $column): ColumnDefinition
    {
        return $this->addColumn('UInt128', $column);
    }

    public function uInt256(string $column): ColumnDefinition
    {
        return $this->addColumn('UInt256', $column);
    }

    /**
     * Alias for int32.
     */
    public function integer(string $column): ColumnDefinition
    {
        return $this->int32($column);
    }

    /**
     * Alias for int64.
     */
    public function bigInteger(string $column): ColumnDefinition
    {
        return $this->int64($column);
    }

    /**
     * Alias for uInt64.
     */
    public function unsignedBigInteger(string $column): ColumnDefinition
    {
        return $this->uInt64($column);
    }

    // =========================================================================
    // Float Column Types
    // =========================================================================

    public function float32(string $column): ColumnDefinition
    {
        return $this->addColumn('Float32', $column);
    }

    public function float64(string $column): ColumnDefinition
    {
        return $this->addColumn('Float64', $column);
    }

    /**
     * Alias for float64.
     */
    public function float(string $column): ColumnDefinition
    {
        return $this->float64($column);
    }

    /**
     * Alias for float64.
     */
    public function double(string $column): ColumnDefinition
    {
        return $this->float64($column);
    }

    // =========================================================================
    // Decimal Column Types
    // =========================================================================

    public function decimal(string $column, int $precision, int $scale): ColumnDefinition
    {
        return $this->addColumn('Decimal', $column, compact('precision', 'scale'));
    }

    public function decimal32(string $column, int $scale): ColumnDefinition
    {
        return $this->addColumn('Decimal32', $column, compact('scale'));
    }

    public function decimal64(string $column, int $scale): ColumnDefinition
    {
        return $this->addColumn('Decimal64', $column, compact('scale'));
    }

    public function decimal128(string $column, int $scale): ColumnDefinition
    {
        return $this->addColumn('Decimal128', $column, compact('scale'));
    }

    public function decimal256(string $column, int $scale): ColumnDefinition
    {
        return $this->addColumn('Decimal256', $column, compact('scale'));
    }

    // =========================================================================
    // String Column Types
    // =========================================================================

    public function string(string $column, ?int $length = null): ColumnDefinition
    {
        return $this->addColumn('String', $column);
    }

    public function fixedString(string $column, int $length): ColumnDefinition
    {
        return $this->addColumn('FixedString', $column, compact('length'));
    }

    /**
     * Alias for string.
     */
    public function text(string $column): ColumnDefinition
    {
        return $this->string($column);
    }

    // =========================================================================
    // Date/Time Column Types
    // =========================================================================

    public function date(string $column): ColumnDefinition
    {
        return $this->addColumn('Date', $column);
    }

    public function date32(string $column): ColumnDefinition
    {
        return $this->addColumn('Date32', $column);
    }

    public function dateTime(string $column, ?string $timezone = null): ColumnDefinition
    {
        return $this->addColumn('DateTime', $column, compact('timezone'));
    }

    public function dateTime64(string $column, int $precision = 3, ?string $timezone = null): ColumnDefinition
    {
        return $this->addColumn('DateTime64', $column, compact('precision', 'timezone'));
    }

    /**
     * Alias for dateTime.
     */
    public function timestamp(string $column, ?string $timezone = null): ColumnDefinition
    {
        return $this->dateTime($column, $timezone);
    }

    /**
     * Add created_at and updated_at timestamps.
     */
    public function timestamps(?string $timezone = null): void
    {
        $this->dateTime('created_at', $timezone)->nullable();
        $this->dateTime('updated_at', $timezone)->nullable();
    }

    // =========================================================================
    // Boolean Column Type
    // =========================================================================

    public function boolean(string $column): ColumnDefinition
    {
        return $this->addColumn('Bool', $column);
    }

    // =========================================================================
    // UUID Column Type
    // =========================================================================

    public function uuid(string $column): ColumnDefinition
    {
        return $this->addColumn('UUID', $column);
    }

    // =========================================================================
    // IP Address Column Types
    // =========================================================================

    public function ipv4(string $column): ColumnDefinition
    {
        return $this->addColumn('IPv4', $column);
    }

    public function ipv6(string $column): ColumnDefinition
    {
        return $this->addColumn('IPv6', $column);
    }

    // =========================================================================
    // Enum Column Types
    // =========================================================================

    public function enum8(string $column, array $values): ColumnDefinition
    {
        return $this->addColumn('Enum8', $column, compact('values'));
    }

    public function enum16(string $column, array $values): ColumnDefinition
    {
        return $this->addColumn('Enum16', $column, compact('values'));
    }

    // =========================================================================
    // Composite Column Types
    // =========================================================================

    public function array(string $column, string $innerType): ColumnDefinition
    {
        return $this->addColumn('Array', $column, ['innerType' => $innerType]);
    }

    public function tuple(string $column, array $types): ColumnDefinition
    {
        return $this->addColumn('Tuple', $column, compact('types'));
    }

    public function map(string $column, string $keyType, string $valueType): ColumnDefinition
    {
        return $this->addColumn('Map', $column, compact('keyType', 'valueType'));
    }

    public function nested(string $column, Closure $callback): ColumnDefinition
    {
        $nestedBlueprint = new static($column);
        $callback($nestedBlueprint);

        return $this->addColumn('Nested', $column, ['nested' => $nestedBlueprint]);
    }

    public function lowCardinality(string $column, string $innerType): ColumnDefinition
    {
        return $this->addColumn('LowCardinality', $column, ['innerType' => $innerType]);
    }

    public function json(string $column): ColumnDefinition
    {
        return $this->addColumn('JSON', $column);
    }

    // =========================================================================
    // Soft Deletes Support
    // =========================================================================

    /**
     * Add a soft delete timestamp column.
     */
    public function softDeletes(string $column = 'deleted_at'): ColumnDefinition
    {
        return $this->dateTime($column)->nullable();
    }

    /**
     * Add a soft delete flag column (for ReplacingMergeTree).
     */
    public function softDeletesFlag(string $column = 'is_deleted'): ColumnDefinition
    {
        return $this->uInt8($column)->default(0);
    }

    // =========================================================================
    // Command Methods
    // =========================================================================

    public function create(): Fluent
    {
        return $this->addCommand('create');
    }

    public function drop(): Fluent
    {
        return $this->addCommand('drop');
    }

    public function dropIfExists(): Fluent
    {
        return $this->addCommand('dropIfExists');
    }

    public function rename(string $to): Fluent
    {
        return $this->addCommand('rename', compact('to'));
    }

    /**
     * Drop columns from the table.
     */
    public function dropColumn(string|array $columns): Fluent
    {
        $columns = is_array($columns) ? $columns : func_get_args();

        return $this->addCommand('dropColumn', compact('columns'));
    }

    /**
     * Rename a column.
     */
    public function renameColumn(string $from, string $to): Fluent
    {
        return $this->addCommand('renameColumn', compact('from', 'to'));
    }

    /**
     * Modify a column type.
     */
    public function modifyColumn(string $column, string $type): Fluent
    {
        return $this->addCommand('modifyColumn', compact('column', 'type'));
    }

    /**
     * Add an index to the table.
     */
    public function addIndex(
        string $name,
        string|array $columns,
        string $type = 'minmax',
        int $granularity = 1
    ): Fluent {
        $columns = (array) $columns;

        return $this->addCommand('addIndex', compact('name', 'columns', 'type', 'granularity'));
    }

    /**
     * Drop an index from the table.
     */
    public function dropIndex(string $name): Fluent
    {
        return $this->addCommand('dropIndex', compact('name'));
    }

    // =========================================================================
    // Internal Methods
    // =========================================================================

    protected function addColumn(string $type, string $name, array $parameters = []): ColumnDefinition
    {
        $column = new ColumnDefinition(
            array_merge(compact('type', 'name'), $parameters)
        );

        $this->columns[] = $column;

        return $column;
    }

    protected function addCommand(string $name, array $parameters = []): Fluent
    {
        $command = new Fluent(array_merge(compact('name'), $parameters));

        $this->commands[] = $command;

        return $command;
    }

    /**
     * Compile the blueprint to SQL statements.
     */
    public function toSql(Connection $connection, ClickhouseGrammar $grammar): array
    {
        $this->addImpliedCommands($grammar);

        $statements = [];

        foreach ($this->commands as $command) {
            $method = 'compile' . ucfirst($command->name);

            if (method_exists($grammar, $method)) {
                $sql = $grammar->{$method}($this, $command, $connection);

                if (! is_null($sql)) {
                    $statements = array_merge($statements, (array) $sql);
                }
            }
        }

        return $statements;
    }

    protected function addImpliedCommands(ClickhouseGrammar $grammar): void
    {
        if (count($this->getAddedColumns()) > 0 && ! $this->creating()) {
            array_unshift($this->commands, $this->createCommand('add'));
        }
    }

    protected function creating(): bool
    {
        foreach ($this->commands as $command) {
            if ($command->name === 'create') {
                return true;
            }
        }

        return false;
    }

    protected function createCommand(string $name, array $parameters = []): Fluent
    {
        return new Fluent(array_merge(compact('name'), $parameters));
    }

    // =========================================================================
    // Getters
    // =========================================================================

    public function getTable(): string
    {
        return $this->table;
    }

    public function getColumns(): array
    {
        return $this->columns;
    }

    public function getAddedColumns(): array
    {
        return array_filter($this->columns, fn ($column) => ! ($column->change ?? false));
    }

    public function getCommands(): array
    {
        return $this->commands;
    }

    public function getEngine(): ?string
    {
        return $this->engine;
    }

    public function getEngineParameters(): array
    {
        return $this->engineParameters;
    }

    public function getOrderBy(): ?array
    {
        return $this->orderBy;
    }

    public function getPartitionBy(): ?array
    {
        return $this->partitionBy;
    }

    public function getPrimaryKey(): ?string
    {
        return $this->primaryKey;
    }

    public function getTtl(): ?int
    {
        return $this->ttl;
    }

    public function getTtlColumn(): ?string
    {
        return $this->ttlColumn;
    }

    public function getOnCluster(): ?string
    {
        return $this->onCluster;
    }

    public function getSettings(): array
    {
        return $this->settings;
    }
}
