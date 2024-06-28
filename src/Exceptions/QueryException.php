<?php

namespace One23\LaravelClickhouse\Exceptions;

class QueryException extends Exception
{
    public static function cannotUpdateEmptyValues()
    {
        return new static('Cannot update with empty values');
    }
}
