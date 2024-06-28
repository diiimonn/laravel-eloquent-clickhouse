<?php

declare(strict_types=1);

namespace One23\LaravelClickhouse;

use Illuminate\Support\ServiceProvider;
use One23\LaravelClickhouse\Database\Connection;
use One23\LaravelClickhouse\Database\Eloquent\Model;

class ClickhouseServiceProvider extends ServiceProvider
{
    public function boot(): void
    {
        $db = $this->app->make('db');

        $db->extend('clickhouse', function($config, $name) {
            $config['name'] = $name;

            $connection = new Connection($config);

            if ($this->app->bound('events')) {
                $connection->setEventDispatcher($this->app['events']);
            }

            return $connection;
        });

        Model::setConnectionResolver($db);
    }
}
