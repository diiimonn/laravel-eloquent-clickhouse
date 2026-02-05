<?php

declare(strict_types=1);

namespace One23\LaravelClickhouse\Database\Schema;

use Closure;
use One23\LaravelClickhouse\Database\Connection;
use One23\LaravelClickhouse\Database\Schema\Grammars\ClickhouseGrammar;

class Builder
{
    /**
     * The database connection instance.
     */
    protected Connection $connection;

    /**
     * The schema grammar instance.
     */
    protected ClickhouseGrammar $grammar;

    /**
     * Create a new schema builder instance.
     */
    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->grammar = new ClickhouseGrammar();
    }

    /**
     * Create a new table on the schema.
     */
    public function create(string $table, Closure $callback): void
    {
        $blueprint = $this->createBlueprint($table);

        $blueprint->create();

        $callback($blueprint);

        $this->build($blueprint);
    }

    /**
     * Modify a table on the schema.
     */
    public function table(string $table, Closure $callback): void
    {
        $blueprint = $this->createBlueprint($table);

        $callback($blueprint);

        $this->build($blueprint);
    }

    /**
     * Drop a table from the schema.
     */
    public function drop(string $table): void
    {
        $blueprint = $this->createBlueprint($table);

        $blueprint->drop();

        $this->build($blueprint);
    }

    /**
     * Drop a table from the schema if it exists.
     */
    public function dropIfExists(string $table): void
    {
        $blueprint = $this->createBlueprint($table);

        $blueprint->dropIfExists();

        $this->build($blueprint);
    }

    /**
     * Rename a table on the schema.
     */
    public function rename(string $from, string $to): void
    {
        $blueprint = $this->createBlueprint($from);

        $blueprint->rename($to);

        $this->build($blueprint);
    }

    /**
     * Determine if the given table exists.
     */
    public function hasTable(string $table): bool
    {
        $database = $this->connection->getDatabaseName();

        $result = $this->connection->select(
            "SELECT name FROM system.tables WHERE database = ? AND name = ?",
            [$database, $table]
        );

        return count($result) > 0;
    }

    /**
     * Determine if the given table has a given column.
     */
    public function hasColumn(string $table, string $column): bool
    {
        return in_array(
            strtolower($column),
            array_map('strtolower', $this->getColumnListing($table))
        );
    }

    /**
     * Determine if the given table has given columns.
     */
    public function hasColumns(string $table, array $columns): bool
    {
        $tableColumns = array_map('strtolower', $this->getColumnListing($table));

        foreach ($columns as $column) {
            if (! in_array(strtolower($column), $tableColumns)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Get the column listing for a given table.
     */
    public function getColumnListing(string $table): array
    {
        $database = $this->connection->getDatabaseName();

        $results = $this->connection->select(
            "SELECT name FROM system.columns WHERE database = ? AND table = ?",
            [$database, $table]
        );

        return array_column($results, 'name');
    }

    /**
     * Get the data type for the given column name.
     */
    public function getColumnType(string $table, string $column): string
    {
        $database = $this->connection->getDatabaseName();

        $results = $this->connection->select(
            "SELECT type FROM system.columns WHERE database = ? AND table = ? AND name = ?",
            [$database, $table, $column]
        );

        if (empty($results)) {
            return '';
        }

        return $results[0]['type'] ?? '';
    }

    /**
     * Get all table names in the database.
     */
    public function getTables(): array
    {
        $database = $this->connection->getDatabaseName();

        $results = $this->connection->select(
            "SELECT name FROM system.tables WHERE database = ?",
            [$database]
        );

        return array_column($results, 'name');
    }

    /**
     * Get the columns for a given table.
     */
    public function getColumns(string $table): array
    {
        $database = $this->connection->getDatabaseName();

        return $this->connection->select(
            "SELECT name, type, default_kind, default_expression, comment
             FROM system.columns
             WHERE database = ? AND table = ?
             ORDER BY position",
            [$database, $table]
        );
    }

    /**
     * Get the indexes for a given table.
     */
    public function getIndexes(string $table): array
    {
        $database = $this->connection->getDatabaseName();

        return $this->connection->select(
            "SELECT name, expr, type, granularity
             FROM system.data_skipping_indices
             WHERE database = ? AND table = ?",
            [$database, $table]
        );
    }

    /**
     * Create a new command set with a Closure.
     */
    protected function createBlueprint(string $table): Blueprint
    {
        return new Blueprint($table);
    }

    /**
     * Execute the blueprint to build / modify the table.
     */
    protected function build(Blueprint $blueprint): void
    {
        $statements = $blueprint->toSql($this->connection, $this->grammar);

        foreach ($statements as $statement) {
            $this->connection->statement($statement);
        }
    }

    /**
     * Get the database connection instance.
     */
    public function getConnection(): Connection
    {
        return $this->connection;
    }

    /**
     * Get the schema grammar instance.
     */
    public function getGrammar(): ClickhouseGrammar
    {
        return $this->grammar;
    }

    /**
     * Drop all tables from the database.
     */
    public function dropAllTables(): void
    {
        $tables = $this->getTables();

        foreach ($tables as $table) {
            $this->drop($table);
        }
    }

    /**
     * Drop all tables from the database if they exist.
     */
    public function dropAllTablesIfExists(): void
    {
        $tables = $this->getTables();

        foreach ($tables as $table) {
            $this->dropIfExists($table);
        }
    }
}
