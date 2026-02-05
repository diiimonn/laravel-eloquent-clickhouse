<?php

namespace One23\LaravelClickhouse\Exceptions;

class NotSupportedException extends Exception
{
    public static function transactions(): static
    {
        return new static('Transactions are not supported by ClickHouse.');
    }

    public static function method(string $method, string $context = 'ClickHouse'): static
    {
        return new static("Method [{$method}] is not supported on {$context} models.");
    }

    public static function incrementing(): static
    {
        return new static('Auto-incrementing IDs are not supported on ClickHouse models.');
    }

    public static function feature(string $feature): static
    {
        return new static("Feature [{$feature}] is not supported by ClickHouse.");
    }

    public static function scope(): static
    {
        return new static('Scope objects are not yet implemented for ClickHouse models. Use Closure-based scopes instead.');
    }

    public static function builderMethod(string $method): static
    {
        return new static("Method [{$method}] is not available on the ClickHouse Eloquent Builder instance.");
    }

    public static function queryBuilderMethod(string $method): static
    {
        return new static("Method [{$method}] is not available on the ClickHouse Query Builder instance.");
    }
}
