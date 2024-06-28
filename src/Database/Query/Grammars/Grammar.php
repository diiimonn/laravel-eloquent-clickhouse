<?php

declare(strict_types=1);

namespace One23\LaravelClickhouse\Database\Query\Grammars;

use Tinderbox\ClickhouseBuilder\Query\Grammar as BaseGrammar;

class Grammar extends BaseGrammar
{
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
