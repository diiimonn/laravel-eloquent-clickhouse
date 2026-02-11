<?php

declare(strict_types=1);

namespace One23\LaravelClickhouse\Database\Query;

use BackedEnum;
use Closure;
use Illuminate\Contracts\Database\Query\Builder as BuilderContract;
use Illuminate\Database\Concerns\BuildsQueries;
use Illuminate\Database\Concerns\ExplainsQueries;
use Illuminate\Database\Eloquent\Relations\Relation;
use Illuminate\Pagination\LengthAwarePaginator;
use Illuminate\Pagination\Paginator;
use Illuminate\Support\Arr;
use Illuminate\Support\Collection;
use Illuminate\Support\LazyCollection;
use Illuminate\Support\Traits\ForwardsCalls;
use RuntimeException;
use Illuminate\Support\Traits\Macroable;
use InvalidArgumentException;
use One23\LaravelClickhouse\Database\Connection;
use One23\LaravelClickhouse\Database\Eloquent\Builder as EloquentBuilder;
use One23\LaravelClickhouse\Database\Query\Grammars\Grammar;
use One23\LaravelClickhouse\Exceptions\QueryException;
use Tinderbox\ClickhouseBuilder\Integrations\Laravel\Builder as BaseBuilder;
use Tinderbox\ClickhouseBuilder\Query\Enums\Operator;
use Tinderbox\ClickhouseBuilder\Query\Expression;
use Tinderbox\ClickhouseBuilder\Query\Identifier;
use Tinderbox\ClickhouseBuilder\Query\Limit;
use UnitEnum;

class Builder extends BaseBuilder implements BuilderContract
{
    use BuildsQueries;
    use ExplainsQueries;
    use ForwardsCalls;
    use Macroable {
        __call as macroCall;
    }

    public $processor;

    public $bindings = [
        'select' => [],
        'from' => [],
        'join' => [],
        'where' => [],
        'groupBy' => [],
        'having' => [],
        'order' => [],
        'union' => [],
        'unionOrder' => [],
    ];

    public $aggregate;

    public $distinct = false;

    public $columns = [];

    /** @var int */
    public $limit;

    /** @var array */
    public $groupLimit;

    /** @var int */
    public $offset;

    /** @var array */
    public $unions;

    /** @var int */
    public $unionLimit;

    /** @var int */
    public $unionOffset;

    /** @var array */
    public $unionOrders;

    public $beforeQueryCallbacks = [];

    protected $afterQueryCallbacks = [];

    public $useWritePdo = false;

    //

    protected ?Limit $baseLimit = null;

    //

    public function __construct(
        Connection $connection,
        ?Grammar $grammar = null
    ) {
        $this->connection = $connection;
        $this->grammar = $grammar ?: new Grammar;
        $this->grammar->setConnection($connection);
    }

    //
    // select
    // selectSub
    //

    /**
     * @param  string  $expression
     */
    public function selectRaw($expression, array $bindings = [])
    {
        $this->addSelect(new Expression($expression));

        if ($bindings) {
            $this->addBinding($bindings, 'select');
        }

        return $this;
    }

    /**
     * Force the query to only return distinct results.
     *
     * @param  mixed  ...$distinct
     * @return $this
     */
    public function distinct($distinct = true)
    {
        if (is_array($distinct)) {
            $this->distinct = $distinct;
        } else {
            $this->distinct = func_num_args() > 1
                ? func_get_args()
                : (bool) $distinct;
        }

        return $this;
    }

    //
    // fromSub
    // fromRaw
    // createSub

    protected function parseSub($query)
    {
        if ($query instanceof self || $query instanceof EloquentBuilder || $query instanceof Relation) {
            $query = $this->prependDatabaseNameIfCrossDatabaseQuery($query);

            return [$query->toSql(), $query->getBindings()];
        } elseif (is_string($query)) {
            return [$query, []];
        } else {
            throw new InvalidArgumentException(
                'A subquery must be a query builder instance, a Closure, or a string.'
            );
        }
    }

    protected function prependDatabaseNameIfCrossDatabaseQuery($query)
    {
        if ($query->getConnection()->getDatabaseName() !==
            $this->getConnection()->getDatabaseName()) {
            $databaseName = $query->getConnection()->getDatabaseName();

            if (! str_starts_with($query->from, $databaseName) && ! str_contains($query->from, '.')) {
                $query->from($databaseName . '.' . $query->from);
            }
        }

        return $query;
    }

    // addSelect
    // distinct
    // from
    // useIndex
    // forceIndex
    // ignoreIndex
    // join
    // joinWhere
    // joinSub
    // joinLateral
    // leftJoinLateral
    // leftJoin
    // leftJoinWhere
    // leftJoinSub
    // rightJoin
    // rightJoinWhere
    // rightJoinSub
    // crossJoin
    // crossJoinSub
    // newJoinClause
    // newJoinLateralClause
    // mergeWheres
    //

    public function where($column, $operator = null, $value = null, string $concatOperator = Operator::AND)
    {
        if (is_array($column)) {
            return $this->addArrayOfWheres($column, $concatOperator);
        }

        // Handle 2-argument form: where('column', 'value') => where('column', '=', 'value')
        if (func_num_args() === 2) {
            $value = $operator;
            $operator = is_array($value) ? Operator::IN : Operator::EQUALS;
        }

        return parent::where($column, $operator, $value, $concatOperator);
    }

    protected function addArrayOfWheres($column, $boolean, $method = 'where')
    {
        foreach ($column as $key => $value) {
            if (is_numeric($key) && is_array($value)) {
                $this->{$method}(...array_values($value));
            } else {
                $this->{$method}($key, '=', $value, $boolean);
            }
        }

        return $this;
    }

    public function prepareValueAndOperator($value, $operator, $useDefault = false): array
    {
        // todo
        return parent::prepareValueAndOperator($value, $operator, $useDefault);
    }

    /**
     * Add a "where not" clause to the query.
     *
     * @param  \Closure|string|array  $column
     * @param  mixed  $operator
     * @param  mixed  $value
     * @param  string  $boolean
     * @return $this
     */
    public function whereNot($column, $operator = null, $value = null, $boolean = 'AND')
    {
        if (is_array($column)) {
            return $this->whereNested(function ($query) use ($column, $operator, $value, $boolean) {
                foreach ($column as $key => $val) {
                    if (is_numeric($key) && is_array($val)) {
                        $query->whereNot(...array_values($val));
                    } else {
                        $query->whereNot($key, '=', $val);
                    }
                }
            }, $boolean, true);
        }

        if ($column instanceof Closure) {
            return $this->whereNested($column, $boolean, true);
        }

        // Handle 2-argument form
        if (func_num_args() === 2) {
            $value = $operator;
            $operator = is_array($value) ? Operator::IN : Operator::EQUALS;
        }

        // Negate the operator
        $operator = $this->negateOperator($operator);

        return parent::where($column, $operator, $value, $boolean);
    }

    /**
     * Add an "or where not" clause to the query.
     *
     * @param  \Closure|string|array  $column
     * @param  mixed  $operator
     * @param  mixed  $value
     * @return $this
     */
    public function orWhereNot($column, $operator = null, $value = null)
    {
        return $this->whereNot($column, $operator, $value, 'OR');
    }

    /**
     * Negate the given operator.
     *
     * @param  string  $operator
     * @return string
     */
    protected function negateOperator($operator)
    {
        return match ($operator) {
            Operator::EQUALS, '=' => Operator::NOT_EQUALS,
            Operator::NOT_EQUALS, '!=' => Operator::EQUALS,
            Operator::LESS, '<' => Operator::GREATER_OR_EQUALS,
            Operator::GREATER, '>' => Operator::LESS_OR_EQUALS,
            Operator::LESS_OR_EQUALS, '<=' => Operator::GREATER,
            Operator::GREATER_OR_EQUALS, '>=' => Operator::LESS,
            Operator::IN => Operator::NOT_IN,
            Operator::NOT_IN => Operator::IN,
            Operator::LIKE => Operator::NOT_LIKE,
            Operator::NOT_LIKE => Operator::LIKE,
            Operator::BETWEEN => Operator::NOT_BETWEEN,
            Operator::NOT_BETWEEN => Operator::BETWEEN,
            default => $operator,
        };
    }

    /**
     * Add a nested where statement to the query.
     *
     * @param  \Closure  $callback
     * @param  string  $boolean
     * @param  bool  $not
     * @return $this
     */
    public function whereNested(Closure $callback, $boolean = 'AND', $not = false)
    {
        $query = $this->forNestedWhere();

        $callback($query);

        if (count($query->getWheres()) > 0) {
            $sql = '(' . $this->grammar->compileWheresComponent($query, $query->getWheres()) . ')';
            if ($not) {
                $sql = 'NOT ' . $sql;
            }
            $this->where(new Expression($sql), null, null, $boolean);
        }

        return $this;
    }

    /**
     * Add a "where column" clause to the query.
     *
     * @param  string|array  $first
     * @param  string|null  $operator
     * @param  string|null  $second
     * @param  string  $boolean
     * @return $this
     */
    public function whereColumn($first, $operator = null, $second = null, $boolean = 'AND')
    {
        // Handle array of column comparisons
        if (is_array($first)) {
            foreach ($first as $item) {
                if (is_array($item)) {
                    $this->whereColumn(...$item);
                }
            }
            return $this;
        }

        // Handle 2-argument form: whereColumn('col1', 'col2')
        if (func_num_args() === 2 || is_null($second)) {
            $second = $operator;
            $operator = Operator::EQUALS;
        }

        return $this->whereRaw(
            $this->grammar->wrap($first) . ' ' . $operator . ' ' . $this->grammar->wrap($second),
            [],
            $boolean
        );
    }

    /**
     * Add an "or where column" clause to the query.
     *
     * @param  string|array  $first
     * @param  string|null  $operator
     * @param  string|null  $second
     * @return $this
     */
    public function orWhereColumn($first, $operator = null, $second = null)
    {
        return $this->whereColumn($first, $operator, $second, 'OR');
    }

    /**
     * Add a raw where clause to the query.
     *
     * @param  string  $expression
     * @param  array  $bindings
     * @param  string  $boolean
     * @return $this
     */
    public function whereRaw(string $expression, array $bindings = [], string $boolean = 'AND')
    {
        // Substitute ? placeholders with actual values for ClickHouse
        if (! empty($bindings)) {
            $bindings = Arr::flatten($bindings);
            $index = 0;
            $expression = preg_replace_callback('/\?/', function () use ($bindings, &$index) {
                $value = $bindings[$index++] ?? null;
                if (is_null($value)) {
                    return 'NULL';
                }
                if (is_string($value)) {
                    return "'" . addslashes($value) . "'";
                }
                if (is_bool($value)) {
                    return $value ? '1' : '0';
                }
                return (string) $value;
            }, $expression);
        }

        return $this->where(new Expression($expression), null, null, $boolean);
    }

    /**
     * Add a raw or where clause to the query.
     *
     * @param  string  $expression
     * @param  array  $bindings
     * @return $this
     */
    public function orWhereRaw(string $expression, array $bindings = [])
    {
        return $this->whereRaw($expression, $bindings, 'OR');
    }

    /**
     * Add a "where in" clause to the query.
     * Supports arrays, closures (subqueries), and Builder instances.
     *
     * @param  string  $column
     * @param  mixed  $values
     * @param  string  $boolean
     * @param  bool  $not
     * @return $this
     */
    public function whereIn($column, $values, $boolean = Operator::AND, $not = false)
    {
        $type = $not ? Operator::NOT_IN : Operator::IN;

        // Handle Closure - execute it to build subquery
        if ($values instanceof Closure) {
            $subQuery = $this->forSubQuery();
            $values($subQuery);
            $values = $subQuery;
        }

        // Handle Builder instance - use as subquery
        if ($values instanceof self || $values instanceof EloquentBuilder) {
            [$sql, $bindings] = $this->parseSub($values);
            $this->addBinding($bindings, 'where');
            return $this->where(
                new Expression($this->grammar->wrap($column) . ' ' . $type . ' (' . $sql . ')'),
                null,
                null,
                $boolean
            );
        }

        // Fallback to parent for arrays and other values
        return parent::whereIn($column, $values, $boolean, $not);
    }

    /**
     * Add an "or where in" clause to the query.
     *
     * @param  string  $column
     * @param  mixed  $values
     * @return $this
     */
    public function orWhereIn($column, $values)
    {
        return $this->whereIn($column, $values, Operator::OR);
    }

    /**
     * Add a "where not in" clause to the query.
     *
     * @param  string  $column
     * @param  mixed  $values
     * @param  string  $boolean
     * @return $this
     */
    public function whereNotIn($column, $values, $boolean = Operator::AND)
    {
        return $this->whereIn($column, $values, $boolean, true);
    }

    /**
     * Add an "or where not in" clause to the query.
     *
     * @param  string  $column
     * @param  mixed  $values
     * @param  string  $boolean
     * @return $this
     */
    public function orWhereNotIn($column, $values, $boolean = Operator::OR)
    {
        return $this->whereNotIn($column, $values, $boolean);
    }

    // whereIntegerInRaw
    // orWhereIntegerInRaw
    // whereIntegerNotInRaw
    // orWhereIntegerNotInRaw

    public function whereNull($columns, $boolean = 'AND', $not = false)
    {
        $type = $not ? 'IS NOT NULL' : 'IS NULL';

        foreach (Arr::wrap($columns) as $column) {
            $this->where(
                new Expression($this->grammar->wrap($column) . ' ' . $type),
                null,
                null,
                $boolean
            );
        }

        return $this;
    }

    public function orWhereNull($column)
    {
        return $this->whereNull($column, 'OR');
    }

    public function whereNotNull($columns, $boolean = 'AND')
    {
        return $this->whereNull($columns, $boolean, true);
    }

    /**
     * Add an "or where not null" clause to the query.
     */
    public function orWhereNotNull($column)
    {
        return $this->whereNotNull($column, 'OR');
    }

    /**
     * Add a "where between" statement to the query.
     */
    public function whereBetween($column, array $values, $boolean = Operator::AND, $not = false)
    {
        $operator = $not ? Operator::NOT_BETWEEN : Operator::BETWEEN;

        return $this->where($column, $operator, array_slice($values, 0, 2), $boolean);
    }

    /**
     * Add an "or where between" statement to the query.
     */
    public function orWhereBetween($column, array $values)
    {
        return $this->whereBetween($column, $values, Operator::OR);
    }

    /**
     * Add a "where not between" statement to the query.
     */
    public function whereNotBetween($column, array $values, $boolean = Operator::AND)
    {
        return $this->whereBetween($column, $values, $boolean, true);
    }

    /**
     * Add an "or where not between" statement to the query.
     */
    public function orWhereNotBetween($column, array $values)
    {
        return $this->whereNotBetween($column, $values, Operator::OR);
    }

    /**
     * Add a "where between columns" statement to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @param  string  $boolean
     * @param  bool  $not
     * @return $this
     */
    public function whereBetweenColumns($column, array $values, $boolean = 'AND', $not = false)
    {
        $type = $not ? 'NOT BETWEEN' : 'BETWEEN';

        return $this->whereRaw(
            $this->grammar->wrap($column) . ' ' . $type . ' ' .
            $this->grammar->wrap($values[0]) . ' AND ' . $this->grammar->wrap($values[1]),
            [],
            $boolean
        );
    }

    /**
     * Add an "or where between columns" statement to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function orWhereBetweenColumns($column, array $values)
    {
        return $this->whereBetweenColumns($column, $values, 'OR');
    }

    /**
     * Add a "where not between columns" statement to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @param  string  $boolean
     * @return $this
     */
    public function whereNotBetweenColumns($column, array $values, $boolean = 'AND')
    {
        return $this->whereBetweenColumns($column, $values, $boolean, true);
    }

    /**
     * Add an "or where not between columns" statement to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function orWhereNotBetweenColumns($column, array $values)
    {
        return $this->whereNotBetweenColumns($column, $values, 'OR');
    }

    /**
     * Add a "where date" statement to the query (ClickHouse uses toDate function).
     */
    public function whereDate($column, $operator, $value = null, $boolean = 'AND')
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        $value = $this->flattenValue($value);

        return $this->whereRaw(
            "toDate({$this->grammar->wrap($column)}) {$operator} ?",
            [$value],
            $boolean
        );
    }

    /**
     * Add an "or where date" statement to the query.
     */
    public function orWhereDate($column, $operator, $value = null)
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        return $this->whereDate($column, $operator, $value, 'OR');
    }

    /**
     * Add a "where year" statement to the query (ClickHouse uses toYear function).
     */
    public function whereYear($column, $operator, $value = null, $boolean = 'AND')
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        $value = $this->flattenValue($value);

        return $this->whereRaw(
            "toYear({$this->grammar->wrap($column)}) {$operator} ?",
            [(int) $value],
            $boolean
        );
    }

    /**
     * Add an "or where year" statement to the query.
     */
    public function orWhereYear($column, $operator, $value = null)
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        return $this->whereYear($column, $operator, $value, 'OR');
    }

    /**
     * Add a "where month" statement to the query (ClickHouse uses toMonth function).
     */
    public function whereMonth($column, $operator, $value = null, $boolean = 'AND')
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        $value = $this->flattenValue($value);

        return $this->whereRaw(
            "toMonth({$this->grammar->wrap($column)}) {$operator} ?",
            [(int) $value],
            $boolean
        );
    }

    /**
     * Add an "or where month" statement to the query.
     */
    public function orWhereMonth($column, $operator, $value = null)
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        return $this->whereMonth($column, $operator, $value, 'OR');
    }

    /**
     * Add a "where day" statement to the query (ClickHouse uses toDayOfMonth function).
     */
    public function whereDay($column, $operator, $value = null, $boolean = 'AND')
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        $value = $this->flattenValue($value);

        return $this->whereRaw(
            "toDayOfMonth({$this->grammar->wrap($column)}) {$operator} ?",
            [(int) $value],
            $boolean
        );
    }

    /**
     * Add an "or where day" statement to the query.
     */
    public function orWhereDay($column, $operator, $value = null)
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        return $this->whereDay($column, $operator, $value, 'OR');
    }

    /**
     * Add a "where time" statement to the query (ClickHouse uses toTime function).
     */
    public function whereTime($column, $operator, $value = null, $boolean = 'AND')
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        $value = $this->flattenValue($value);

        return $this->whereRaw(
            "toTime({$this->grammar->wrap($column)}) {$operator} ?",
            [$value],
            $boolean
        );
    }

    /**
     * Add an "or where time" statement to the query.
     */
    public function orWhereTime($column, $operator, $value = null)
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        return $this->whereTime($column, $operator, $value, 'OR');
    }

    public function groupByRaw($sql, array $bindings = [])
    {
        $this->addGroupBy(new Expression($sql));

        $this->addBinding($bindings, 'groupBy');

        return $this;
    }

    // having
    // orHaving
    // havingNested
    // addNestedHavingQuery
    // havingNull
    // orHavingNull
    // havingNotNull
    // orHavingNotNull
    // havingBetween
    // havingRaw
    // orHavingRaw
    // orderBy - inherited from parent
    // orderByDesc - inherited from parent
    // orderByRaw - inherited from parent

    /**
     * Add an "order by" clause for a timestamp to the query (descending).
     *
     * @param  string  $column
     * @return $this
     */
    public function latest($column = 'created_at')
    {
        return $this->orderBy($column, 'desc');
    }

    /**
     * Add an "order by" clause for a timestamp to the query (ascending).
     *
     * @param  string  $column
     * @return $this
     */
    public function oldest($column = 'created_at')
    {
        return $this->orderBy($column, 'asc');
    }

    /**
     * Put the query's results in random order.
     * Uses ClickHouse's rand() function.
     *
     * @param  string|int  $seed
     * @return $this
     */
    public function inRandomOrder($seed = '')
    {
        if ($seed !== '') {
            return $this->orderByRaw('rand(' . (int) $seed . ')');
        }

        return $this->orderByRaw('rand()');
    }

    public function skip($value)
    {
        return $this->offset($value);
    }

    public function offset($value)
    {
        $property = $this->unions ? 'unionOffset' : 'offset';

        $this->$property = max(0, (int)$value);

        //

        if ($property === 'offset') {
            if (
                ($this->limit ?? 0) > 0
            ) {
                $this->baseLimit = new Limit(
                    $this->limit,
                    $value
                );
            } else {
                $this->baseLimit = null;
            }
        }

        return $this;
    }

    final public function take(int $limit, ?int $offset = null)
    {
        return $this->limit($limit, $offset);
    }

    public function limit(int $value, ?int $offset = null)
    {
        $property = $this->unions ? 'unionLimit' : 'limit';

        if ($value >= 0) {
            $this->$property = ! is_null($value) ? (int)$value : null;
        }

        if ($property === 'limit') {
            $this->baseLimit = new Limit(
                $value,
                ! is_null($offset)
                    ? $offset
                    : $this->offset
            );
        }

        return $this;
    }

    // groupLimit

    public function forPage($page, $perPage = 15)
    {
        return $this->offset(($page - 1) * $perPage)->limit($perPage);
    }

    public function forPageBeforeId($perPage = 15, $lastId = 0, $column = 'id')
    {
        $this->orders = $this->removeExistingOrdersFor($column);

        if (! is_null($lastId)) {
            $this->where($column, '<', $lastId);
        }

        return $this->orderBy($column, 'desc')
            ->limit($perPage);
    }

    public function forPageAfterId($perPage = 15, $lastId = 0, $column = 'id')
    {
        $this->orders = $this->removeExistingOrdersFor($column);

        if (! is_null($lastId)) {
            $this->where($column, '>', $lastId);
        }

        return $this->orderBy($column, 'asc')
            ->limit($perPage);
    }

    public function reorder($column = null, $direction = 'asc')
    {
        $this->orders = null;
        $this->unionOrders = null;
        $this->bindings['order'] = [];
        $this->bindings['unionOrder'] = [];

        if ($column) {
            return $this->orderBy($column, $direction);
        }

        return $this;
    }

    protected function removeExistingOrdersFor($column)
    {
        return Collection::make($this->orders)
            ->reject(function($order) use ($column) {
                return isset($order['column'])
                    ? $order['column'] === $column : false;
            })->values()->all();
    }

    // union
    // unionAll
    // lock
    // lockForUpdate
    // sharedLock

    public function beforeQuery(callable $callback)
    {
        $this->beforeQueryCallbacks[] = $callback;

        return $this;
    }

    public function applyBeforeQueryCallbacks()
    {
        foreach ($this->beforeQueryCallbacks as $callback) {
            $callback($this);
        }

        $this->beforeQueryCallbacks = [];
    }

    public function afterQuery(Closure $callback)
    {
        $this->afterQueryCallbacks[] = $callback;

        return $this;
    }

    public function applyAfterQueryCallbacks($result)
    {
        foreach ($this->afterQueryCallbacks as $afterQueryCallback) {
            $result = $afterQueryCallback($result) ?: $result;
        }

        return $result;
    }

    public function toSql(): string
    {
        $this->applyBeforeQueryCallbacks();

        return $this->grammar->compileSelect($this);
    }

    public function toRawSql()
    {
        return $this->grammar->substituteBindingsIntoRawSql(
            $this->toSql(), $this->connection->prepareBindings($this->getBindings())
        );
    }

    // find
    // findOr
    // value
    // rawValue
    // soleValue

    public function get($columns = ['*']): Collection
    {
        $items = collect($this->onceWithColumns(Arr::wrap($columns), function() {
            return parent::get();
        }));

        return $this->applyAfterQueryCallbacks(
            isset($this->groupLimit) ? $this->withoutGroupLimitKeys($items) : $items
        );
    }

    // runSelect

    protected function withoutGroupLimitKeys($items)
    {
        $keysToRemove = ['laravel_row'];

        if (is_string($this->groupLimit['column'])) {
            $column = last(explode('.', $this->groupLimit['column']));

            $keysToRemove[] = '@laravel_group := ' . $this->grammar->wrap($column);
            $keysToRemove[] = '@laravel_group := ' . $this->grammar->wrap('pivot_' . $column);
        }

        $items->each(function($item) use ($keysToRemove) {
            foreach ($keysToRemove as $key) {
                unset($item->$key);
            }
        });

        return $items;
    }

    public function paginate($perPage = 15, $columns = ['*'], $pageName = 'page', $page = null, $total = null): LengthAwarePaginator
    {
        $page = $page ?: Paginator::resolveCurrentPage($pageName);

        $total = value($total) ?? $this->getCountForPagination();

        $perPage = $perPage instanceof Closure ? $perPage($total) : $perPage;

        $results = $total ? $this->forPage($page, $perPage)->get($columns) : collect();

        return $this->paginator($results, $total, $perPage, $page, [
            'path' => Paginator::resolveCurrentPath(),
            'pageName' => $pageName,
        ]);
    }

    public function simplePaginate($perPage = 15, $columns = ['*'], $pageName = 'page', $page = null)
    {
        $page = $page ?: Paginator::resolveCurrentPage($pageName);

        $this->offset(($page - 1) * $perPage)->limit($perPage + 1);

        return $this->simplePaginator($this->get($columns), $perPage, $page, [
            'path' => Paginator::resolveCurrentPath(),
            'pageName' => $pageName,
        ]);
    }

    // cursorPaginate
    // ensureOrderForCursorPagination

    public function getCountForPagination($columns = ['*'])
    {
        $results = $this->runPaginationCountQuery($columns);

        // Once we have run the pagination count query, we will get the resulting count and
        // take into account what type of query it was. When there is a group by we will
        // just return the count of the entire results set since that will be correct.
        if (! isset($results[0])) {
            return 0;
        } elseif (is_object($results[0])) {
            return (int)$results[0]->aggregate;
        }

        return (int)array_change_key_case((array)$results[0])['aggregate'];
    }

    protected function runPaginationCountQuery($columns = ['*'])
    {
        if ($this->groups || $this->havings) {
            $clone = $this->cloneForPaginationCount();

            if (empty($clone->columns) && ! empty($this->joins)) {
                $clone->select($this->from . '.*');
            }

            return $this->newQuery()
                ->from(new Expression('(' . $clone->toSql() . ') as `aggregate_table`'))
                ->mergeBindings($clone)
                ->setAggregate('count', $this->withoutSelectAliases($columns))
                ->get()->all();
        }

        $without = $this->unions ? ['orders', 'limit', 'offset'] : ['columns', 'orders', 'limit', 'offset'];

        return $this->cloneWithout($without)
            ->cloneWithoutBindings($this->unions ? ['order'] : ['select', 'order'])
            ->setAggregate('count', $this->withoutSelectAliases($columns))
            ->get()->all();
    }

    protected function cloneForPaginationCount(): Builder
    {
        return $this->cloneWithout(['orders', 'limit', 'offset'])
            ->cloneWithoutBindings(['order']);
    }

    protected function withoutSelectAliases(array $columns)
    {
        return array_map(function($column) {
            return is_string($column) && ($aliasPosition = stripos($column, ' as ')) !== false
                ? substr($column, 0, $aliasPosition) : $column;
        }, $columns);
    }

    // cursor
    // enforceOrderBy
    // pluck
    // stripTableForPluck
    // pluckFromObjectColumn
    // pluckFromArrayColumn
    // implode
    // exists
    // doesntExist
    // existsOr
    // doesntExistOr

    public function count($column = '*'): int
    {
        $builder = $this->getCountQuery($column);
        $result = $builder->get();

        if (count($this->groups) > 0) {
            return count($result);
        }

        return (int)($result[0]['count'] ?? 0);
    }

    /**
     * Retrieve the minimum value of a given column.
     */
    public function min($column): mixed
    {
        return $this->aggregate(__FUNCTION__, [$column]);
    }

    /**
     * Retrieve the maximum value of a given column.
     */
    public function max($column): mixed
    {
        return $this->aggregate(__FUNCTION__, [$column]);
    }

    /**
     * Retrieve the sum of the values of a given column.
     */
    public function sum($column): mixed
    {
        $result = $this->aggregate(__FUNCTION__, [$column]);

        return $result ?: 0;
    }

    /**
     * Retrieve the average of the values of a given column.
     */
    public function avg($column): mixed
    {
        return $this->aggregate(__FUNCTION__, [$column]);
    }

    /**
     * Alias for the "avg" method.
     */
    public function average($column): mixed
    {
        return $this->avg($column);
    }

    /**
     * Execute an aggregate function on the database.
     */
    public function aggregate(string $function, array $columns = ['*']): mixed
    {
        $results = $this->cloneWithout($this->unions ? [] : ['columns'])
            ->cloneWithoutBindings($this->unions ? [] : ['select'])
            ->setAggregate($function, $columns)
            ->get($columns);

        if (! $results->isEmpty()) {
            return array_change_key_case((array) $results[0])['aggregate'];
        }

        return null;
    }

    /**
     * Execute a numeric aggregate function on the database.
     */
    public function numericAggregate(string $function, array $columns = ['*']): int|float
    {
        $result = $this->aggregate($function, $columns);

        if (! $result) {
            return 0;
        }

        if (is_int($result) || is_float($result)) {
            return $result;
        }

        return str_contains((string) $result, '.') ? (float) $result : (int) $result;
    }

    /**
     * Determine if any rows exist for the current query.
     */
    public function exists(): bool
    {
        $this->applyBeforeQueryCallbacks();

        $results = $this->limit(1)->get(['*']);

        return $results->count() > 0;
    }

    /**
     * Determine if no rows exist for the current query.
     */
    public function doesntExist(): bool
    {
        return ! $this->exists();
    }

    /**
     * Execute the given callback if no rows exist for the current query.
     */
    public function existsOr(Closure $callback): mixed
    {
        return $this->exists() ? true : $callback();
    }

    /**
     * Execute the given callback if rows exist for the current query.
     */
    public function doesntExistOr(Closure $callback): mixed
    {
        return $this->doesntExist() ? true : $callback();
    }

    /**
     * Get a single column's value from the first result of a query.
     */
    public function value($column): mixed
    {
        $result = $this->first([$column]);

        return $result ? data_get($result, $column) : null;
    }

    /**
     * Get an array with the values of a given column.
     */
    public function pluck($column, $key = null): Collection
    {
        $queryResult = $this->get(is_null($key) ? [$column] : [$column, $key]);

        return is_null($key)
            ? $queryResult->pluck($column)
            : $queryResult->pluck($column, $key);
    }

    /**
     * Concatenate values of a given column as a string.
     *
     * @param  string  $column
     * @param  string  $glue
     * @return string
     */
    public function implode($column, $glue = '')
    {
        return $this->pluck($column)->implode($glue);
    }

    /**
     * Get a single column's value from the first result of a query if it's the sole matching record.
     *
     * @param  string  $column
     * @return mixed
     */
    public function soleValue($column)
    {
        $result = $this->sole([$column]);

        return $result ? data_get($result, $column) : null;
    }

    /**
     * Execute the query and get the first result if it's the sole matching record.
     *
     * @param  array|string  $columns
     * @return mixed
     *
     * @throws \Illuminate\Database\RecordsNotFoundException
     * @throws \Illuminate\Database\MultipleRecordsFoundException
     */
    public function sole($columns = ['*'])
    {
        $result = $this->take(2)->get($columns);

        $count = $result->count();

        if ($count === 0) {
            throw new \Illuminate\Database\RecordsNotFoundException();
        }

        if ($count > 1) {
            throw new \Illuminate\Database\MultipleRecordsFoundException($count);
        }

        return $result->first();
    }

    /**
     * Get a raw expression value from the first result of a query.
     *
     * @param  string  $expression
     * @param  array  $bindings
     * @return mixed
     */
    public function rawValue($expression, array $bindings = [])
    {
        $result = $this->selectRaw($expression, $bindings)->first();

        if (! $result) {
            return null;
        }

        $result = (array) $result;

        return reset($result);
    }

    /**
     * Create a raw database expression.
     *
     * @param  mixed  $value
     * @return \Tinderbox\ClickhouseBuilder\Query\Expression
     */
    public function raw($value)
    {
        return new Expression($value);
    }

    /**
     * Chunk the results of the query.
     */
    public function chunk(int $count, callable $callback): bool
    {
        $this->enforceOrderBy();

        $page = 1;

        do {
            $results = $this->forPage($page, $count)->get();

            $countResults = $results->count();

            if ($countResults == 0) {
                break;
            }

            if ($callback($results, $page) === false) {
                return false;
            }

            unset($results);

            $page++;
        } while ($countResults == $count);

        return true;
    }

    /**
     * Chunk the results of a query by comparing IDs.
     */
    public function chunkById(int $count, callable $callback, ?string $column = null, ?string $alias = null): bool
    {
        $column = $column ?? $this->defaultKeyName();
        $alias = $alias ?? $column;

        $lastId = null;

        do {
            $clone = clone $this;

            $results = $clone->forPageAfterId($count, $lastId, $column)->get();

            $countResults = $results->count();

            if ($countResults == 0) {
                break;
            }

            if ($callback($results) === false) {
                return false;
            }

            $lastId = data_get($results->last(), $alias);

            if ($lastId === null) {
                throw new RuntimeException("The chunkById operation was aborted because the [{$alias}] column is not present in the query result.");
            }

            unset($results);
        } while ($countResults == $count);

        return true;
    }

    /**
     * Query lazily, by chunks of the given size.
     */
    public function lazy(int $chunkSize = 1000): LazyCollection
    {
        $this->enforceOrderBy();

        return LazyCollection::make(function () use ($chunkSize) {
            $page = 1;

            while (true) {
                $results = $this->forPage($page++, $chunkSize)->get();

                foreach ($results as $result) {
                    yield $result;
                }

                if ($results->count() < $chunkSize) {
                    return;
                }
            }
        });
    }

    /**
     * Query lazily, by chunking the results of a query by comparing IDs.
     */
    public function lazyById(int $chunkSize = 1000, ?string $column = null, ?string $alias = null): LazyCollection
    {
        $column = $column ?? $this->defaultKeyName();
        $alias = $alias ?? $column;

        return LazyCollection::make(function () use ($chunkSize, $column, $alias) {
            $lastId = null;

            while (true) {
                $clone = clone $this;

                $results = $clone->forPageAfterId($chunkSize, $lastId, $column)->get();

                foreach ($results as $result) {
                    yield $result;
                }

                if ($results->count() < $chunkSize) {
                    return;
                }

                $lastId = data_get($results->last(), $alias);
            }
        });
    }

    /**
     * Query lazily, by chunking the results of a query by comparing IDs in descending order.
     */
    public function lazyByIdDesc(int $chunkSize = 1000, ?string $column = null, ?string $alias = null): LazyCollection
    {
        $column = $column ?? $this->defaultKeyName();
        $alias = $alias ?? $column;

        return LazyCollection::make(function () use ($chunkSize, $column, $alias) {
            $lastId = null;

            while (true) {
                $clone = clone $this;

                $results = $clone->forPageBeforeId($chunkSize, $lastId, $column)->get();

                foreach ($results as $result) {
                    yield $result;
                }

                if ($results->count() < $chunkSize) {
                    return;
                }

                $lastId = data_get($results->last(), $alias);
            }
        });
    }

    /**
     * Throw an exception if the query doesn't have an orderBy clause.
     */
    protected function enforceOrderBy(): void
    {
        if (empty($this->orders) && empty($this->unionOrders)) {
            throw new RuntimeException('You must specify an orderBy clause when using this function.');
        }
    }

    protected function setAggregate($function, $columns)
    {
        $this->aggregate = compact('function', 'columns');

        if (empty($this->groups)) {
            $this->orders = null;

            $this->bindings['order'] = [];
        }

        // grammar compileAggregate

        $column = $this->grammar__columnize($this->aggregate['columns']);

        if (is_array($this->distinct)) {
            $column = 'distinct ' . $this->grammar__columnize($this->distinct);
        } elseif ($this->distinct && $column !== '*') {
            $column = 'distinct ' . $column;
        }

        $this->selectRaw(
            $this->aggregate['function'] . '(' . $column . ') as aggregate'
        );

        //

        return $this;
    }

    protected function onceWithColumns($columns, $callback)
    {
        $original = $this->columns;

        if (is_null($original)) {
            $this->columns = $columns;
        }

        $result = $callback();

        $this->columns = $original;

        return $result;
    }

    public function insert(array $values, bool $skipSort = false): bool
    {
        return parent::insert($values, $skipSort);
    }

    // insertOrIgnore
    // insertGetId
    // insertUsing
    // insertOrIgnoreUsing

    /**
     * @return int
     */
    public function update(array $values)
    {
        $this->applyBeforeQueryCallbacks();

        $values = collect($values)->map(function($value) {
            if (! $value instanceof Builder) {
                return ['value' => $value, 'bindings' => $value];
            }

            [$query, $bindings] = $this->parseSub($value);

            return ['value' => new Expression("({$query})"), 'bindings' => fn() => $bindings];
        });

        if ($values->isEmpty()) {
            throw QueryException::cannotUpdateEmptyValues();
        }

        $table = $this->grammar->wrap($this->getFrom()->getTable());

        $cluster = '';
        if (! is_null($this->getOnCluster())) {
            $cluster = " ON CLUSTER {$this->getOnCluster()}";
        }

        $columns = collect($values)->map(function($value, $key) {
            return $this->grammar->wrap($key) . ' = ' . $this->grammar->parameter($value);
        })->implode(', ');

        $where = $this->grammar->compileWheresComponent($this, $this->getWheres());

        $sql = "ALTER TABLE {$table} {$cluster} UPDATE {$columns} {$where};";

        return $this->connection->statement($sql) ? 1 : 0;
    }

    // updateFrom
    // updateOrInsert
    // upsert
    // increment
    // incrementEach
    // decrement
    // decrementEach

    /**
     * @return int
     */
    public function delete($id = null)
    {
        // If an ID is passed to the method, we will set the where clause to check the
        // ID to let developers to simply and quickly remove a single row from this
        // database without manually specifying the "where" clauses on the query.
        if (! is_null($id)) {
            $this->where($this->from . '.id', '=', $id);
        }

        $this->applyBeforeQueryCallbacks();

        $table = $this->grammar->wrap($this->getFrom()->getTable());

        $cluster = '';
        if (! is_null($this->getOnCluster())) {
            $cluster = " ON CLUSTER {$this->getOnCluster()}";
        }

        $where = $this->grammar->compileWheresComponent($this, $this->getWheres());

        $sql = <<<SQL
ALTER TABLE {$table} {$cluster} DELETE {$where};
SQL;

        return $this->connection->statement($sql) ? 1 : 0;
    }

    // truncate

    public function newQuery(): self
    {
        return new static($this->connection, $this->grammar);
    }

    protected function forSubQuery()
    {
        return $this->newQuery();
    }

    /**
     * Create a new query instance for nested where condition.
     *
     * @return static
     */
    public function forNestedWhere()
    {
        return $this->newQuery()->from($this->getFrom());
    }

    public function getColumns(): array
    {
        // todo
        return ! empty($this->columns)
            ? $this->columns
            : [];
    }

    // raw
    // getUnionBuilders
    // setBindings

    public function getBindings()
    {
        return Arr::flatten($this->bindings);
    }

    public function getRawBindings()
    {
        return $this->bindings;
    }

    public function setBindings(array $bindings, $type = 'where')
    {
        if (! array_key_exists($type, $this->bindings)) {
            throw new InvalidArgumentException("Invalid binding type: {$type}.");
        }

        $this->bindings[$type] = $bindings;

        return $this;
    }

    public function addBinding($value, $type = 'where')
    {
        if (! array_key_exists($type, $this->bindings)) {
            throw new InvalidArgumentException("Invalid binding type: {$type}.");
        }

        if (is_array($value)) {
            $this->bindings[$type] = array_values(array_map(
                [$this, 'castBinding'],
                array_merge($this->bindings[$type], $value),
            ));
        } else {
            $this->bindings[$type][] = $this->castBinding($value);
        }

        return $this;
    }

    public function castBinding($value)
    {
        if ($value instanceof UnitEnum) {
            return $value instanceof BackedEnum ? $value->value : $value->name;
        }

        return $value;
    }

    public function mergeBindings(self $query)
    {
        $this->bindings = array_merge_recursive($this->bindings, $query->bindings);

        return $this;
    }

    public function cleanBindings(array $bindings)
    {
        return collect($bindings)
            ->reject(function($binding) {
                return $binding instanceof Expression;
            })
            ->map([$this, 'castBinding'])
            ->values()
            ->all();
    }

    protected function flattenValue($value)
    {
        return is_array($value) ? head(Arr::flatten($value)) : $value;
    }

    protected function defaultKeyName()
    {
        return 'id';
    }

    public function getConnection(): Connection
    {
        return $this->connection;
    }

    public function getProcessor()
    {
        return $this->processor;
    }

    public function getGrammar()
    {
        return $this->grammar;
    }

    public function useWritePdo()
    {
        $this->useWritePdo = true;

        return $this;
    }

    protected function isQueryable($value)
    {
        return $value instanceof self ||
            $value instanceof EloquentBuilder ||
            $value instanceof Relation ||
            $value instanceof Closure;
    }

    public function clone(): Builder
    {
        return clone $this;
    }

    public function cloneWithout(array $properties)
    {
        return tap($this->clone(), function ($clone) use ($properties) {
            foreach ($properties as $key => $value) {
                // Support both formats:
                // ['columns', 'orders'] - numeric keys, set to default
                // ['columns' => [], 'limit' => null] - associative, set to specified value
                if (is_int($key)) {
                    $property = $value;
                    $clone->{$property} = is_array($clone->{$property}) ? [] : null;
                } else {
                    $clone->{$key} = $value;
                }
            }
        });
    }

    public function cloneWithoutBindings(array $except)
    {
        return tap($this->clone(), function($clone) use ($except) {
            foreach ($except as $type) {
                $clone->bindings[$type] = [];
            }
        });
    }

    public function dump(...$args)
    {
        dump(
            $this->toSql(),
            $this->getBindings(),
            ...$args,
        );

        return $this;
    }

    public function dumpRawSql()
    {
        dump($this->toRawSql());

        return $this;
    }

    public function dd()
    {
        dd($this->toSql(), $this->getBindings());
    }

    public function ddRawSql()
    {
        dd($this->toRawSql());
    }

    public function __call($method, $parameters)
    {
        if (static::hasMacro($method)) {
            return $this->macroCall($method, $parameters);
        }

        // todo
        //        if (str_starts_with($method, 'where')) {
        //            return $this->dynamicWhere($method, $parameters);
        //        }

        static::throwBadMethodCallException($method);
    }

    //
    //
    //

    public function getOrders(): array
    {
        return ! empty($this->orders)
            ? $this->orders
            : [];
    }

    public function getGroups(): array
    {
        return ! empty($this->groups)
            ? $this->groups
            : [];
    }

    public function getHavings(): array
    {
        return ! empty($this->havings)
            ? $this->havings
            : [];
    }

    public function getUnions(): array
    {
        return ! empty($this->unions)
            ? $this->unions
            : [];
    }

    public function first($columns = ['*'])
    {
        return $this->take(1)->get($columns)->first();
    }

    public function getCountQuery($column = '*')
    {
        $without = ['columns' => [], 'limit' => null, 'offset' => null, 'baseLimit' => null];

        if (empty($this->groups)) {
            $without['orders'] = [];
        }

        $col = $column === '*' ? '*' : $this->grammar->wrap(new Identifier($column));

        $distinct = '';
        if (is_array($this->distinct)) {
            $col = implode(', ', array_map(fn($c) => $this->grammar->wrap(new Identifier($c)), $this->distinct));
            $distinct = 'DISTINCT ';
        } elseif ($this->distinct && $column !== '*') {
            $distinct = 'DISTINCT ';
        }

        return $this->cloneWithout($without)
            ->selectRaw("count({$distinct}{$col}) as `count`");
    }

    protected function grammar__columnize(array $columns)
    {
        return implode(', ', array_map(function ($column) {
            if ($column === '*') {
                return '*';
            }

            return $this->grammar->wrap(new Identifier($column));
        }, $columns));
    }

    public function getLimit(): ?Limit
    {
        return $this->baseLimit;
    }
}
