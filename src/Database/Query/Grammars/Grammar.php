<?php

declare(strict_types=1);

namespace One23\LaravelClickhouse\Database\Query\Grammars;

use RuntimeException;
use Tinderbox\ClickhouseBuilder\Query\Grammar as BaseGrammar;

class Grammar extends BaseGrammar
{
    protected $connection = null;

    public function setConnection($connection)
    {
        $this->connection = $connection;

        return $this;
    }

    public function escape($value, $binary = false)
    {
        if (is_null($this->connection)) {
            throw new RuntimeException("The database driver's grammar implementation does not support escaping values.");
        }

        return $this->connection->escape($value, $binary);
    }

    public function substituteBindingsIntoRawSql($sql, $bindings)
    {
        $bindings = array_map(fn($value) => $this->escape($value), $bindings);

        $query = '';

        $isStringLiteral = false;

        for ($i = 0; $i < strlen($sql); $i++) {
            $char = $sql[$i];
            $nextChar = $sql[$i + 1] ?? null;

            // Single quotes can be escaped as '' according to the SQL standard while
            // MySQL uses \'. Postgres has operators like ?| that must get encoded
            // in PHP like ??|. We should skip over the escaped characters here.
            if (in_array($char . $nextChar, ["\'", "''", '??'])) {
                $query .= $char . $nextChar;
                $i += 1;
            } elseif ($char === "'") { // Starting / leaving string literal...
                $query .= $char;
                $isStringLiteral = ! $isStringLiteral;
            } elseif ($char === '?' && ! $isStringLiteral) { // Substitutable binding...
                $query .= array_shift($bindings) ?? '?';
            } else { // Normal character...
                $query .= $char;
            }
        }

        return $query;
    }

    public function getDateFormat()
    {
        return 'Y-m-d H:i:s';
    }

    public function compileInsertValues($values)
    {
        return implode(', ', array_map(function($value) {
            return '(' . implode(', ', array_map(function($value) {
                if (is_array($value)) {
                    return '[' . implode(
                        ',',
                        array_map([$this, 'wrap'], $value)
                    ) . ']';
                }

                return $this->wrap($value);
            }, $value)) . ')';
        }, $values));
    }
}
