# Laravel Clickhouse Eloquent

## Install

```shell
composer require one23/laravel-clickhouse
```

Add provider to bootstrap/providers.php

```php
return [
    //...
    One23\LaravelClickhouse\ClickhouseServiceProvider::class,
    //...
];
```

Add connection to config/database.php

```php
return [
    'connections' => [
        //...
        'clickhouse' => [
            'driver' => 'clickhouse',
            'host' => env('CLICKHOUSE_HOST', 'localhost'),
            'port' => env('CLICKHOUSE_PORT', 8123),
            'database' => env('CLICKHOUSE_DATABASE'),
            'username' => env('CLICKHOUSE_USERNAME'),
            'password' => env('CLICKHOUSE_PASSWORD'),
            'options' => [
                'timeout' => 15,
                'protocol' => 'http',
            ],
        ],
        //...
    ]
];
```

Create model. Example:

```php
use One23\LaravelClickhouse\Database\Eloquent\Model;

class StatisticDaily extends Model
{
    protected $table = 'mv_statistic_daily';

    protected $casts = [
        'date' => 'date',
        'link_id' => 'int',
        'cnt' => 'int',
        'uniq_ip' => 'int',
    ];

    protected $primaryKey = null;

    //
}
```

Use model. Example:

```php
Clickhouse\StatisticDaily::query()
    ->where('date', '=', $dt)
    ->delete();

Clickhouse\StatisticDaily::query()
    ->where('link_id', '=', $OLink->getId())
    ->whereBetween('date', [
        $from->startOfDay()->toDateString(),
        $to->endOfDay()->toDateString(),
    ])
    ->groupBy('date')
    ->selectRaw(implode(', ', [
        '`date`',
        'SUM(`cnt`) AS `cnt`',
        'SUM(`cnt_uniq_ip`) AS `cnt_uniq_ip`',

        'SUM(`cnt_uniq`) AS `cnt_uniq`',
        'SUM(`cnt_mobile`) AS `cnt_mobile`',
        'AVG(`avg_latency`) AS `avg_latency`',
        'MIN(NULLIF(`min_latency`, 0)) AS `min_latency`',
        'MAX(`max_latency`) AS `max_latency`',
        'AVG(`quantile_latency`) AS `quantile_latency`',
    ]))
    ->get();

Clickhouse\StatisticDaily::query()
    ->whereIn('link_id', $linkIds)
    ->whereBetween('date', [
        $this->from
            ->toDateString(),
        $this->to
            ->toDateString(),
    ])
    ->selectRaw(implode(', ', [
        'SUM(`statistic_daily`.`cnt`) as `cnt`',
        'SUM(`statistic_daily`.`cnt_uniq_ip`) as `cnt_uniq_ip`',
    ]))
    ->first();
```

## Todo

- Tests
- Deep where's
- Rewrite Grammar
- ...

## Security

If you discover any security related issues, please email eugene@krivoruchko.info instead of using the issue tracker.

## License

[MIT](https://github.com/FlexIDK/language-detection/LICENSE)
