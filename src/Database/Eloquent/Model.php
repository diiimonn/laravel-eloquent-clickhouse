<?php

declare(strict_types=1);

namespace One23\LaravelClickhouse\Database\Eloquent;

use ArrayAccess;
use Illuminate\Contracts\Support\Arrayable;
use Illuminate\Contracts\Support\CanBeEscapedWhenCastToString;
use Illuminate\Contracts\Support\Jsonable;
use Illuminate\Database\ConnectionResolverInterface as Resolver;
use Illuminate\Database\Eloquent\Collection as Collection;
use Illuminate\Database\Eloquent\Concerns;
use Illuminate\Database\Eloquent\JsonEncodingException;
use Illuminate\Database\Eloquent\MassAssignmentException;
use Illuminate\Database\Eloquent\MissingAttributeException;
use Illuminate\Support\Str;
use Illuminate\Support\Traits\ForwardsCalls;
use JsonSerializable;
use Stringable;

abstract class Model implements Arrayable, ArrayAccess, CanBeEscapedWhenCastToString, Jsonable, JsonSerializable, Stringable
{
    use Concerns\GuardsAttributes,
        Concerns\HasAttributes,
        Concerns\HasEvents,
        Concerns\HasGlobalScopes,
        Concerns\HasRelationships,
        Concerns\HasTimestamps,
        Concerns\HasUniqueIds,
        Concerns\HidesAttributes,
        ForwardsCalls;

    protected $connection = 'clickhouse';

    protected $table;

    protected $primaryKey = 'id';

    protected $keyType = 'int';

    public readonly bool $incrementing;

    protected $with = [];

    protected $withCount = [];

    protected $perPage = 15;

    public $exists = false;

    public $wasRecentlyCreated = false;

    protected $escapeWhenCastingToString = false;

    protected static $resolver;

    protected static $dispatcher;

    protected static $booted = [];

    protected static $traitInitializers = [];

    protected static $globalScopes = [];

    protected static $ignoreOnTouch = [];

    protected static $modelsShouldPreventLazyLoading = false;

    protected static $modelsShouldPreventSilentlyDiscardingAttributes = false;

    protected static $discardedAttributeViolationCallback;

    protected static $modelsShouldPreventAccessingMissingAttributes = false;

    protected static $isBroadcasting = false;

    const CREATED_AT = 'created_at';

    const UPDATED_AT = 'updated_at';

    /**
     * Create a new Eloquent model instance.
     *
     * @return void
     */
    public function __construct(array $attributes = [])
    {
        $this->incrementing = false;

        //

        $this->bootIfNotBooted();

        $this->initializeTraits();

        $this->syncOriginal();

        $this->fill($attributes);
    }

    /**
     * Check if the model needs to be booted and if so, do it.
     *
     * @return void
     */
    protected function bootIfNotBooted()
    {
        if (! isset(static::$booted[static::class])) {
            static::$booted[static::class] = true;

            $this->fireModelEvent('booting', false);

            static::booting();
            static::boot();
            static::booted();

            $this->fireModelEvent('booted', false);
        }
    }

    protected static function booting()
    {
        //
    }

    /**
     * The "booting" method of the model.
     *
     * @return void
     */
    protected static function boot()
    {
        static::bootTraits();
    }

    /**
     * Boot all of the bootable traits on the model.
     *
     * @return void
     */
    protected static function bootTraits()
    {
        $class = static::class;

        $booted = [];

        static::$traitInitializers[$class] = [];

        foreach (class_uses_recursive($class) as $trait) {
            $method = 'boot' . class_basename($trait);

            if (method_exists($class, $method) && ! in_array($method, $booted)) {
                forward_static_call([$class, $method]);

                $booted[] = $method;
            }

            if (method_exists($class, $method = 'initialize' . class_basename($trait))) {
                static::$traitInitializers[$class][] = $method;

                static::$traitInitializers[$class] = array_unique(
                    static::$traitInitializers[$class]
                );
            }
        }
    }

    protected function initializeTraits()
    {
        foreach (static::$traitInitializers[static::class] as $method) {
            $this->{$method}();
        }
    }

    protected static function booted()
    {
        //
    }

    /**
     * Clear the list of booted models so they will be re-booted.
     *
     * @return void
     */
    public static function clearBootedModels()
    {
        static::$booted = [];
    }

    /**
     * Fill the model with an array of attributes.
     *
     * @return $this
     *
     * @throws \Illuminate\Database\Eloquent\MassAssignmentException
     */
    public function fill(array $attributes)
    {
        $totallyGuarded = $this->totallyGuarded();

        $fillable = $this->fillableFromArray($attributes);

        foreach ($fillable as $key => $value) {
            $key = $this->removeTableFromKey($key);

            // The developers may choose to place some attributes in the "fillable" array
            // which means only those attributes may be set through mass assignment to
            // the model, and all others will just get ignored for security reasons.
            if ($this->isFillable($key)) {
                $this->setAttribute($key, $value);
            } elseif ($totallyGuarded) {
                throw new MassAssignmentException(sprintf(
                    'Add [%s] to fillable property to allow mass assignment on [%s].',
                    $key, get_class($this)
                ));
            }
        }

        return $this;
    }

    /**
     * Fill the model with an array of attributes. Force mass assignment.
     *
     * @return $this
     */
    public function forceFill(array $attributes)
    {
        return static::unguarded(fn() => $this->fill($attributes));
    }

    public function qualifyColumn($column)
    {
        if (str_contains($column, '.')) {
            return $column;
        }

        return $this->getTable() . '.' . $column;
    }

    public function qualifyColumns($columns)
    {
        return collect($columns)->map(function($column) {
            return $this->qualifyColumn($column);
        })->all();
    }

    /**
     * Create a new instance of the given model.
     *
     * @param  array  $attributes
     * @param  bool  $exists
     * @return static
     */
    public function newInstance($attributes = [], $exists = false)
    {
        $model = new static;

        $model->exists = $exists;

        $model->setConnection(
            $this->getConnectionName()
        );

        $model->setTable($this->getTable());

        $model->mergeCasts($this->casts);

        $model->fill((array)$attributes);

        return $model;
    }

    /**
     * Create a new model instance that is existing.
     *
     * @return static
     */
    public function newFromBuilder(array $attributes = [], ?string $connection = null)
    {
        $model = $this->newInstance([], true);

        $model->setRawAttributes((array)$attributes, true);

        $model->setConnection($connection ?: $this->getConnectionName());

        $model->fireModelEvent('retrieved', false);

        return $model;
    }

    /**
     * @return Collection|static[]
     */
    public static function all($columns = ['*'])
    {
        return static::query()->get(
            is_array($columns) ? $columns : func_get_args()
        );
    }

    /**
     * @param  array|string  $relations
     * @return Builder|static
     */
    public static function with($relations)
    {
        return static::query()->with(
            is_string($relations) ? func_get_args() : $relations
        );
    }

    public static function query(): Builder
    {
        return (new static)->newQuery();
    }

    public function newQuery(): Builder
    {
        return $this->registerGlobalScopes($this->newQueryWithoutScopes());
    }

    public function newModelQuery()
    {
        return $this->newEloquentBuilder(
            $this->newBaseQueryBuilder()
        )->setModel($this);
    }

    public function newQueryWithoutRelationships()
    {
        return $this->registerGlobalScopes($this->newModelQuery());
    }

    public function registerGlobalScopes($builder)
    {
        foreach ($this->getGlobalScopes() as $identifier => $scope) {
            $builder->withGlobalScope($identifier, $scope);
        }

        return $builder;
    }

    public function newQueryWithoutScopes(): Builder
    {
        return $this->newModelQuery()
            ->with($this->with);
        //            ->withCount($this->withCount)
    }

    public function newQueryWithoutScope($scope)
    {
        return $this->newQuery()->withoutGlobalScope($scope);
    }

    public function newQueryForRestoration($ids)
    {
        return $this->newQueryWithoutScopes()->whereKey($ids);
    }

    public function newEloquentBuilder($query)
    {
        return new Builder($query);
    }

    protected function newBaseQueryBuilder()
    {
        return $this->getConnection()->query();
    }

    public function newCollection(array $models = [])
    {
        return new Collection($models);
    }

    public function hasNamedScope($scope)
    {
        return method_exists($this, 'scope' . ucfirst($scope));
    }

    public function callNamedScope($scope, array $parameters = [])
    {
        return $this->{'scope' . ucfirst($scope)}(...$parameters);
    }

    public function toArray()
    {
        return array_merge($this->attributesToArray(), $this->relationsToArray());
    }

    public function toJson($options = 0)
    {
        $json = json_encode($this->jsonSerialize(), $options);

        if (json_last_error() !== JSON_ERROR_NONE) {
            throw JsonEncodingException::forModel($this, json_last_error_msg());
        }

        return $json;
    }

    public function jsonSerialize(): mixed
    {
        return $this->toArray();
    }

    public function is($model)
    {
        return ! is_null($model) &&
            $this->getKey() === $model->getKey() &&
            $this->getTable() === $model->getTable() &&
            $this->getConnectionName() === $model->getConnectionName();
    }

    public function isNot($model)
    {
        return ! $this->is($model);
    }

    public function getConnection()
    {
        return static::resolveConnection($this->getConnectionName());
    }

    public function getConnectionName()
    {
        return $this->connection;
    }

    public function setConnection($name)
    {
        $this->connection = $name;

        return $this;
    }

    public static function resolveConnection($connection = null)
    {
        return static::$resolver->connection($connection);
    }

    public static function getConnectionResolver()
    {
        return static::$resolver;
    }

    public static function setConnectionResolver(Resolver $resolver)
    {
        static::$resolver = $resolver;
    }

    public static function unsetConnectionResolver()
    {
        static::$resolver = null;
    }

    public function getTable()
    {
        return $this->table ?? Str::snake(Str::pluralStudly(class_basename($this)));
    }

    public function setTable($table)
    {
        $this->table = $table;

        return $this;
    }

    public function getKeyName()
    {
        return $this->primaryKey;
    }

    public function setKeyName($key)
    {
        $this->primaryKey = $key;

        return $this;
    }

    public function getQualifiedKeyName()
    {
        return $this->qualifyColumn($this->getKeyName());
    }

    public function getKeyType()
    {
        return $this->keyType;
    }

    public function setKeyType($type)
    {
        $this->keyType = $type;

        return $this;
    }

    public function getIncrementing()
    {
        return $this->incrementing;
    }

    public function setIncrementing($value)
    {
        throw new \Exception('Is not supported on Clickhouse models.');
    }

    public function getKey()
    {
        return $this->getAttribute($this->getKeyName());
    }

    public function getForeignKey(): string
    {
        return Str::snake(class_basename($this)) . '_' . $this->getKeyName();
    }

    public function getPerPage(): int
    {
        return $this->perPage;
    }

    public function setPerPage($perPage)
    {
        $this->perPage = $perPage;

        return $this;
    }

    public static function preventsAccessingMissingAttributes()
    {
        return static::$modelsShouldPreventAccessingMissingAttributes;
    }

    public function __get($key)
    {
        return $this->getAttribute($key);
    }

    public function __set($key, $value)
    {
        $this->setAttribute($key, $value);
    }

    public function offsetExists($offset): bool
    {
        try {
            return ! is_null($this->getAttribute($offset));
        } catch (MissingAttributeException) {
            return false;
        }
    }

    public function offsetGet($offset): mixed
    {
        return $this->getAttribute($offset);
    }

    public function offsetSet($offset, $value): void
    {
        $this->setAttribute($offset, $value);
    }

    public function offsetUnset($offset): void
    {
        unset($this->attributes[$offset], $this->relations[$offset]);
    }

    public function __isset($key)
    {
        return $this->offsetExists($key);
    }

    public function __unset($key)
    {
        $this->offsetUnset($key);
    }

    /**
     * Handle dynamic method calls into the model.
     *
     * @param  string  $method
     * @param  array  $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        $methods = [
            'withoutTouching', 'withoutTouchingOn', 'isIgnoringTouch',
            'shouldBeStrict',
            'preventLazyLoading', 'handleLazyLoadingViolationUsing',
            'preventSilentlyDiscardingAttributes', 'handleDiscardedAttributeViolationUsing',
            'preventAccessingMissingAttributes', 'handleMissingAttributeViolationUsing',
            'withoutBroadcasting',
            'on', 'onWriteConnection',

            'load', 'loadMorph', 'loadMissing', 'loadAggregate',
            'loadCount', 'loadMax', 'loadMin', 'loadSum', 'loadAvg',
            'loadExists',
            'loadMorphAggregate',
            'loadMorphCount', 'loadMorphMax', 'loadMorphMin', 'loadMorphSum', 'loadMorphAvg',
            'increment', 'decrement',
            'incrementOrDecrement',
            'incrementQuietly', 'decrementQuietly',
            'update', 'updateOrFail', 'updateQuietly',
            'push', 'pushQuietly',
            'save', 'saveQuietly', 'saveOrFail', 'finishSave', 'performUpdate',
            'setKeysForSelectQuery', 'getKeyForSelectQuery', 'setKeysForSaveQuery', 'getKeyForSaveQuery',
            'performInsert', 'insertAndSetId',
            'destroy', 'delete', 'deleteQuietly', 'deleteOrFail', 'forceDelete', 'performDeleteOnModel',
            'newPivot',
            'fresh', 'refresh', 'replicate', 'replicateQuietly',
            'resolveSoftDeletableRouteBinding', 'resolveSoftDeletableChildRouteBinding', 'resolveChildRouteBindingQuery',
            'childRouteBindingRelationshipName', 'resolveRouteBindingQuery',
            'preventsLazyLoading', 'preventsSilentlyDiscardingAttributes',
        ];

        if (in_array($method, $methods)) {
            dd("Method [{$method}] is not supported on Clickhouse models.");
        }

        //
        //
        //

        if (in_array($method, ['increment', 'decrement', 'incrementQuietly', 'decrementQuietly'])) {
            return $this->$method(...$parameters);
        }

        if ($resolver = $this->relationResolver(static::class, $method)) {
            return $resolver($this);
        }

        if (Str::startsWith($method, 'through') &&
            method_exists($this, $relationMethod = Str::of($method)->after('through')->lcfirst()->toString())) {
            return $this->through($relationMethod);
        }

        return $this->forwardCallTo($this->newQuery(), $method, $parameters);
    }

    public static function __callStatic($method, $parameters)
    {
        return (new static)->$method(...$parameters);
    }

    public function __toString()
    {
        return $this->escapeWhenCastingToString
            ? e($this->toJson())
            : $this->toJson();
    }

    public function escapeWhenCastingToString($escape = true)
    {
        $this->escapeWhenCastingToString = $escape;

        return $this;
    }

    public function __sleep()
    {
        $this->mergeAttributesFromCachedCasts();

        $this->classCastCache = [];
        $this->attributeCastCache = [];

        return array_keys(get_object_vars($this));
    }

    public function __wakeup()
    {
        $this->bootIfNotBooted();

        $this->initializeTraits();
    }

    //
    //
    //

    /**
     * Remove the table name from a given key.
     *
     * @param  string  $key
     * @return string
     */
    protected function removeTableFromKey($key)
    {
        return Str::contains($key, '.') ? last(explode('.', $key)) : $key;
    }
}
