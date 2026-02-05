<?php

declare(strict_types=1);

namespace One23\LaravelClickhouse\Database\Schema;

use Illuminate\Support\Fluent;

/**
 * @method $this nullable(bool $value = true)
 * @method $this default(mixed $value)
 * @method $this comment(string $comment)
 * @method $this codec(string $codec)
 * @method $this ttl(string $expression)
 * @method $this after(string $column)
 * @method $this first()
 * @method $this change()
 */
class ColumnDefinition extends Fluent
{
    /**
     * Allow null values for the column.
     */
    public function nullable(bool $value = true): static
    {
        $this->attributes['nullable'] = $value;

        return $this;
    }

    /**
     * Set the default value for the column.
     */
    public function default(mixed $value): static
    {
        $this->attributes['default'] = $value;

        return $this;
    }

    /**
     * Set a default expression for the column.
     */
    public function defaultExpression(string $expression): static
    {
        $this->attributes['defaultExpression'] = $expression;

        return $this;
    }

    /**
     * Set a materialized expression for the column.
     */
    public function materialized(string $expression): static
    {
        $this->attributes['materialized'] = $expression;

        return $this;
    }

    /**
     * Set an alias expression for the column.
     */
    public function alias(string $expression): static
    {
        $this->attributes['alias'] = $expression;

        return $this;
    }

    /**
     * Add a comment to the column.
     */
    public function comment(string $comment): static
    {
        $this->attributes['comment'] = $comment;

        return $this;
    }

    /**
     * Set the codec for the column (ClickHouse compression).
     */
    public function codec(string $codec): static
    {
        $this->attributes['codec'] = $codec;

        return $this;
    }

    /**
     * Set TTL expression for the column.
     */
    public function ttl(string $expression): static
    {
        $this->attributes['ttl'] = $expression;

        return $this;
    }

    /**
     * Place the column after another column.
     */
    public function after(string $column): static
    {
        $this->attributes['after'] = $column;

        return $this;
    }

    /**
     * Place the column first in the table.
     */
    public function first(): static
    {
        $this->attributes['first'] = true;

        return $this;
    }

    /**
     * Mark the column for modification.
     */
    public function change(): static
    {
        $this->attributes['change'] = true;

        return $this;
    }
}
