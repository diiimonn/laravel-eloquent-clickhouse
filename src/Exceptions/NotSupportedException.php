<?php

namespace One23\LaravelClickhouse\Exceptions;

class NotSupportedException extends Exception
{
    public static function transactions()
    {
        return new static('Transactions is not supported by Clickhouse');
    }
}
