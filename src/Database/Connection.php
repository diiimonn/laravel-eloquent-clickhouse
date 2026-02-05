<?php

declare(strict_types=1);

namespace One23\LaravelClickhouse\Database;

use Illuminate\Support\Arr;
use One23\LaravelClickhouse\Database\Query\Builder;
use One23\LaravelClickhouse\Database\Schema\Builder as SchemaBuilder;
use One23\LaravelClickhouse\Exceptions\NotSupportedException;

class Connection extends \Tinderbox\ClickhouseBuilder\Integrations\Laravel\Connection
{
    protected $config = [];

    public function getConfig($option = null)
    {
        return Arr::get($this->config, $option);
    }

    public function getQueryGrammar()
    {
        return $this->query()->getGrammar();
    }

    public function query()
    {
        return new Builder($this);
    }

    /**
     * Get a schema builder instance for the connection.
     */
    public function getSchemaBuilder(): SchemaBuilder
    {
        return new SchemaBuilder($this);
    }

    /**
     * Get the name of the connected database.
     */
    public function getDatabaseName(): string
    {
        return $this->getConfig('database') ?? 'default';
    }

    /**
     * Get the table prefix for the connection.
     */
    public function getTablePrefix(): string
    {
        return $this->getConfig('prefix') ?? '';
    }

    //
    //
    //

    public function __call($method, $parameters)
    {
        //        if (in_array(
        //            $method,
        //            ['beginTransaction', 'commit', 'rollBack', 'transaction', 'transactionLevel', 'afterCommit']
        //        )) {
        //            throw NotSupportedException::transactions();
        //        }

        parent::__call($method, $parameters);
    }

    public function transaction(\Closure $callback, $attempts = 1)
    {
        throw NotSupportedException::transactions();
    }

    public function beginTransaction()
    {
        throw NotSupportedException::transactions();
    }

    public function commit()
    {
        throw NotSupportedException::transactions();
    }

    public function rollBack($toLevel = null)
    {
        throw NotSupportedException::transactions();
    }

    public function transactionLevel()
    {
        throw NotSupportedException::transactions();
    }

    public function afterCommit($callback)
    {
        throw NotSupportedException::transactions();
    }

    /**
     * Escape a value for safe SQL embedding.
     *
     * @param  mixed  $value
     * @param  bool  $binary
     * @return string
     */
    public function escape($value, $binary = false)
    {
        if (is_null($value)) {
            return 'NULL';
        }

        if (is_bool($value)) {
            return $value ? '1' : '0';
        }

        if (is_int($value) || is_float($value)) {
            return (string) $value;
        }

        if (is_array($value)) {
            return '[' . implode(', ', array_map([$this, 'escape'], $value)) . ']';
        }

        // String escaping for ClickHouse
        // Escape backslashes first, then single quotes
        $value = str_replace('\\', '\\\\', (string) $value);
        $value = str_replace("'", "\\'", $value);

        return "'" . $value . "'";
    }

    /**
     * Prepare the query bindings for execution.
     *
     * @param  array  $bindings
     * @return array
     */
    public function prepareBindings(array $bindings)
    {
        foreach ($bindings as $key => $value) {
            if ($value instanceof \DateTimeInterface) {
                $bindings[$key] = $value->format($this->getQueryGrammar()->getDateFormat());
            } elseif (is_bool($value)) {
                $bindings[$key] = (int) $value;
            }
        }

        return $bindings;
    }
}
