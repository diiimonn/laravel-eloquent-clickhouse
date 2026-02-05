# Laravel Eloquent ClickHouse

A Laravel Eloquent-style query builder and ORM for ClickHouse database.

## Requirements

- PHP 8.2+
- Laravel 10.x / 11.x / 12.x

## Installation

```shell
composer require diiimonn/laravel-eloquent-clickhouse
```

Add provider to `bootstrap/providers.php`:

```php
return [
    // ...
    One23\LaravelClickhouse\ClickhouseServiceProvider::class,
];
```

Add connection to `config/database.php`:

```php
return [
    'connections' => [
        // ...
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
    ],
];
```

## Creating Models

```php
<?php

namespace App\Models\Clickhouse;

use One23\LaravelClickhouse\Database\Eloquent\Model;

class Event extends Model
{
    protected $table = 'events';

    protected $primaryKey = 'id';

    // ClickHouse doesn't support auto-increment
    public $incrementing = false;

    protected $casts = [
        'created_at' => 'datetime',
        'user_id' => 'int',
        'amount' => 'float',
        'is_active' => 'bool',
    ];
}
```

## Query Builder Methods

### Basic Select

```php
use App\Models\Clickhouse\Event;

// Get all records
$events = Event::all();

// Get specific columns
$events = Event::all(['id', 'name', 'created_at']);

// Using query builder
$events = Event::query()->get();

// Select specific columns
$events = Event::query()
    ->select('id', 'name', 'created_at')
    ->get();

// Select with raw expressions
$events = Event::query()
    ->selectRaw('id, name, toDate(created_at) as date')
    ->get();
```

### Where Clauses

#### Basic Where

```php
// With operator
Event::where('status', '=', 'active')->get();

// Without operator (defaults to '=')
Event::where('status', 'active')->get();

// With array (multiple conditions)
Event::where([
    ['status', '=', 'active'],
    ['user_id', '>', 100],
])->get();

// Array shorthand
Event::where([
    'status' => 'active',
    'type' => 'click',
])->get();
```

#### Or Where

```php
Event::where('status', 'active')
    ->orWhere('status', 'pending')
    ->get();

Event::where('type', 'click')
    ->orWhere('amount', '>', 100)
    ->get();
```

#### Where Not

```php
// Exclude specific value
Event::whereNot('status', 'deleted')->get();

// With operator
Event::whereNot('amount', '<', 10)->get();

// Or where not
Event::where('type', 'click')
    ->orWhereNot('status', 'deleted')
    ->get();
```

#### Where In / Not In

```php
// Simple array
Event::whereIn('status', ['active', 'pending'])->get();
Event::whereNotIn('status', ['deleted', 'archived'])->get();

// With subquery (Closure)
Event::whereIn('user_id', function ($query) {
    $query->select('id')
        ->from('users')
        ->where('is_premium', 1);
})->get();

Event::whereNotIn('category_id', function ($query) {
    $query->select('id')
        ->from('categories')
        ->where('is_hidden', 1);
})->get();

// Or variants
Event::where('type', 'click')
    ->orWhereIn('status', ['active', 'pending'])
    ->get();
```

#### Where Null / Not Null

```php
Event::whereNull('deleted_at')->get();
Event::whereNotNull('confirmed_at')->get();

// Or variants
Event::where('status', 'active')
    ->orWhereNull('deleted_at')
    ->get();
```

#### Where Between

```php
// Value between
Event::whereBetween('amount', [100, 500])->get();
Event::whereNotBetween('amount', [0, 10])->get();

// Or variants
Event::where('type', 'purchase')
    ->orWhereBetween('amount', [100, 500])
    ->get();
```

#### Where Between Columns

```php
// Column between two other columns
Event::whereBetweenColumns('amount', ['min_amount', 'max_amount'])->get();
Event::whereNotBetweenColumns('price', ['min_price', 'max_price'])->get();

// Or variants
Event::where('type', 'sale')
    ->orWhereBetweenColumns('amount', ['min_amount', 'max_amount'])
    ->get();
```

#### Where Column (Compare Columns)

```php
// Compare two columns
Event::whereColumn('updated_at', '>', 'created_at')->get();

// Without operator (defaults to '=')
Event::whereColumn('first_name', 'last_name')->get();

// Or variant
Event::where('status', 'active')
    ->orWhereColumn('amount', '>', 'min_amount')
    ->get();

// Multiple column comparisons
Event::whereColumn([
    ['first_name', '!=', 'last_name'],
    ['updated_at', '>', 'created_at'],
])->get();
```

#### Where Date / Time

```php
// ClickHouse toDate() function
Event::whereDate('created_at', '2024-01-15')->get();
Event::whereDate('created_at', '>', '2024-01-01')->get();

// Year (toYear)
Event::whereYear('created_at', 2024)->get();
Event::whereYear('created_at', '>=', 2023)->get();

// Month (toMonth)
Event::whereMonth('created_at', 12)->get();

// Day (toDayOfMonth)
Event::whereDay('created_at', 15)->get();

// Time (toTime)
Event::whereTime('created_at', '>', '10:00:00')->get();

// Or variants
Event::where('type', 'click')
    ->orWhereDate('created_at', '2024-01-15')
    ->get();
```

#### Where Raw

```php
Event::whereRaw('amount > 100 AND status = ?', ['active'])->get();
Event::whereRaw("toYear(created_at) = 2024")->get();

// Or variant
Event::where('type', 'click')
    ->orWhereRaw('amount > ?', [100])
    ->get();
```

#### Nested Where (Grouping)

```php
Event::where('status', 'active')
    ->where(function ($query) {
        $query->where('type', 'click')
              ->orWhere('type', 'view');
    })
    ->get();

// Generates: WHERE status = 'active' AND (type = 'click' OR type = 'view')
```

### Ordering

```php
// Basic ordering
Event::orderBy('created_at', 'desc')->get();
Event::orderBy('name', 'asc')->get();

// Multiple orders
Event::orderBy('status', 'asc')
    ->orderBy('created_at', 'desc')
    ->get();

// Shortcuts for timestamp columns
Event::latest()->get();                    // ORDER BY created_at DESC
Event::latest('updated_at')->get();        // ORDER BY updated_at DESC
Event::oldest()->get();                    // ORDER BY created_at ASC
Event::oldest('published_at')->get();      // ORDER BY published_at ASC

// Random order (ClickHouse rand())
Event::inRandomOrder()->get();
Event::inRandomOrder(12345)->get();        // With seed

// Raw ordering
Event::orderByRaw('rand()')->get();
Event::orderByRaw('length(name) DESC')->get();
```

### Grouping

```php
Event::select('status')
    ->selectRaw('COUNT(*) as count')
    ->groupBy('status')
    ->get();

// Multiple columns
Event::selectRaw('status, type, COUNT(*) as count')
    ->groupBy('status', 'type')
    ->get();

// Raw grouping
Event::selectRaw('toDate(created_at) as date, COUNT(*) as count')
    ->groupByRaw('toDate(created_at)')
    ->get();
```

### Having

```php
Event::selectRaw('user_id, COUNT(*) as count')
    ->groupBy('user_id')
    ->having('count', '>', 10)
    ->get();

// Without operator
Event::selectRaw('status, COUNT(*) as count')
    ->groupBy('status')
    ->having('count', 5)  // defaults to '='
    ->get();

// Or having
Event::selectRaw('user_id, SUM(amount) as total')
    ->groupBy('user_id')
    ->having('total', '>', 100)
    ->orHaving('total', '<', 10)
    ->get();

// Having raw
Event::selectRaw('user_id, COUNT(*) as count')
    ->groupBy('user_id')
    ->havingRaw('count > 10')
    ->get();
```

### Limit and Offset

```php
Event::limit(10)->get();
Event::take(10)->get();          // Alias for limit

Event::offset(20)->limit(10)->get();
Event::skip(20)->take(10)->get();  // Aliases

// Pagination shortcut
Event::forPage(3, 10)->get();    // Page 3, 10 per page
```

### Distinct

```php
Event::distinct()->get();

Event::select('status')
    ->distinct()
    ->get();
```

### Aggregates

```php
// Count
Event::count();
Event::count('id');
Event::where('status', 'active')->count();

// Sum
Event::sum('amount');
Event::where('type', 'purchase')->sum('amount');

// Avg
Event::avg('amount');
Event::average('amount');  // Alias

// Min / Max
Event::min('amount');
Event::max('amount');

// With conditions
Event::where('status', 'active')
    ->where('type', 'purchase')
    ->sum('amount');
```

### Pagination

```php
// Length-aware pagination
$events = Event::where('status', 'active')
    ->paginate(15);

// Simple pagination (no total count)
$events = Event::where('status', 'active')
    ->simplePaginate(15);

// Custom page
$events = Event::paginate(15, ['*'], 'page', 2);
```

### Chunking

```php
// Process in chunks
Event::orderBy('id')->chunk(1000, function ($events) {
    foreach ($events as $event) {
        // Process each event
    }
});

// Chunk by ID
Event::chunkById(1000, function ($events) {
    foreach ($events as $event) {
        // Process each event
    }
});
```

### Lazy Collections

```php
// Memory-efficient iteration
Event::orderBy('id')->lazy()->each(function ($event) {
    // Process each event
});

Event::lazyById(1000)->each(function ($event) {
    // Process
});
```

### Finding Records

```php
// By primary key
$event = Event::find(1);
$events = Event::find([1, 2, 3]);

// First record
$event = Event::first();
$event = Event::where('status', 'active')->first();

// First or fail
$event = Event::findOrFail(1);
$event = Event::where('status', 'active')->firstOrFail();

// First or new
$event = Event::firstOrNew(['email' => 'test@example.com']);

// Get single column value
$name = Event::where('id', 1)->value('name');

// Get column values as array
$names = Event::pluck('name');
$names = Event::pluck('name', 'id');  // Keyed by id
```

### Exists

```php
$exists = Event::where('email', 'test@example.com')->exists();
$doesntExist = Event::where('email', 'test@example.com')->doesntExist();
```

### Insert

```php
// Single insert
Event::query()->insert([
    'user_id' => 1,
    'type' => 'click',
    'created_at' => now(),
]);

// Bulk insert
Event::query()->insert([
    ['user_id' => 1, 'type' => 'click', 'created_at' => now()],
    ['user_id' => 2, 'type' => 'view', 'created_at' => now()],
    ['user_id' => 3, 'type' => 'click', 'created_at' => now()],
]);

// Via model
$event = new Event();
$event->user_id = 1;
$event->type = 'click';
$event->save();
```

### Update

```php
// Mass update (ALTER TABLE ... UPDATE)
Event::where('status', 'pending')
    ->update(['status' => 'active']);

// Via model
$event = Event::find(1);
$event->status = 'active';
$event->save();
```

### Delete

```php
// Mass delete (ALTER TABLE ... DELETE)
Event::where('status', 'deleted')->delete();
Event::where('created_at', '<', '2023-01-01')->delete();

// By primary key
Event::query()->delete(1);

// Via model
$event = Event::find(1);
$event->delete();
```

### Raw Expressions

```php
use One23\LaravelClickhouse\Database\Query\Expression;

Event::selectRaw('*, toDate(created_at) as date')->get();

Event::select('*')
    ->selectRaw('formatDateTime(created_at, ?) as formatted', ['%Y-%m-%d'])
    ->get();
```

### Joins

```php
// ClickHouse-specific joins
Event::query()
    ->select('events.*', 'users.name')
    ->join('users', 'ANY', 'LEFT', ['user_id'])
    ->get();

// With alias
Event::query()
    ->join('users', 'ALL', 'INNER', ['user_id'], false, 'u')
    ->get();
```

### ClickHouse-Specific Features

#### PreWhere (Optimized filtering)

```php
// PreWhere is optimized for filtering before reading data
Event::query()
    ->preWhere('date', '>=', '2024-01-01')
    ->where('status', 'active')
    ->get();
```

#### Sample

```php
// Sample 10% of data
Event::query()
    ->sample(0.1)
    ->get();
```

#### Final

```php
// For ReplacingMergeTree tables
Event::query()
    ->from('events', null, true)  // FINAL
    ->get();
```

#### Global In

```php
// For distributed queries
Event::whereGlobalIn('user_id', [1, 2, 3])->get();
Event::whereGlobalNotIn('user_id', [1, 2, 3])->get();
```

#### Format

```php
// Specify output format
Event::query()
    ->format('JSONEachRow')
    ->get();
```

### ClickHouse Aggregation Functions

```php
// Using selectRaw for ClickHouse-specific functions
Event::query()
    ->selectRaw(implode(', ', [
        'user_id',
        'argMax(status, updated_at) as latest_status',
        'argMin(amount, created_at) as first_amount',
        'groupArray(type) as types',
        'uniq(ip_address) as unique_ips',
        'quantile(0.95)(response_time) as p95_response',
    ]))
    ->groupBy('user_id')
    ->get();

// Count distinct
Event::query()
    ->selectRaw('uniqExact(user_id) as unique_users')
    ->first();

// Array functions
Event::query()
    ->selectRaw('arrayJoin(tags) as tag')
    ->groupBy('tag')
    ->selectRaw('tag, count() as count')
    ->get();
```

### Debugging

```php
// Get SQL query
$sql = Event::where('status', 'active')->toSql();

// Get raw SQL with bindings
$sql = Event::where('status', 'active')->toRawSql();

// Dump query
Event::where('status', 'active')->dump();
Event::where('status', 'active')->dd();
```

### Relationships

```php
class User extends Model
{
    public function events(): HasMany
    {
        return $this->hasMany(Event::class);
    }
}

class Event extends Model
{
    public function user(): BelongsTo
    {
        return $this->belongsTo(User::class);
    }
}

// Usage
$user = User::with('events')->find(1);
$events = $user->events;

$event = Event::with('user')->first();
$userName = $event->user->name;
```

## Important Notes

### ClickHouse Limitations

1. **No Transactions** - ClickHouse doesn't support transactions
2. **No Auto-Increment** - Always set `$incrementing = false` on models
3. **Eventual Consistency** - Data may not be immediately visible after insert
4. **ALTER TABLE for Update/Delete** - Uses `ALTER TABLE ... UPDATE/DELETE` which is asynchronous

### Performance Tips

1. Use `preWhere` for filtering on partition keys
2. Use `FINAL` modifier sparingly (performance impact)
3. Prefer batch inserts over single row inserts
4. Use appropriate ClickHouse-specific aggregate functions
5. Consider using materialized views for complex aggregations

## Testing

```shell
composer test
```

## Security

If you discover any security related issues, please email the maintainer instead of using the issue tracker.

## License

MIT License. See [LICENSE](LICENSE) for more information.
