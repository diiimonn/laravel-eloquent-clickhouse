<?php

declare(strict_types=1);

namespace One23\LaravelClickhouse\Database;

use Illuminate\Support\Arr;
use One23\LaravelClickhouse\Database\Query\Builder;
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
}
