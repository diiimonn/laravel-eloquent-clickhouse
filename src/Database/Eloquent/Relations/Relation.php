<?php

declare(strict_types=1);

namespace One23\LaravelClickhouse\Database\Eloquent\Relations;

use Closure;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Support\Traits\ForwardsCalls;
use Illuminate\Support\Traits\Macroable;
use One23\LaravelClickhouse\Database\Eloquent\Builder;
use One23\LaravelClickhouse\Database\Eloquent\Model;

abstract class Relation
{
    use ForwardsCalls, Macroable {
        __call as macroCall;
    }

    /**
     * The Eloquent query builder instance.
     */
    protected Builder $query;

    /**
     * The parent model instance.
     */
    protected Model $parent;

    /**
     * The related model instance.
     */
    protected Model $related;

    /**
     * Indicates if the relation is adding constraints.
     */
    protected static bool $constraints = true;

    /**
     * An array to map class names to their morph names in the database.
     *
     * @var array
     */
    public static array $morphMap = [];

    /**
     * Prevents morph relationships without a morph map.
     */
    protected static bool $requireMorphMap = false;

    /**
     * The count of self joins.
     */
    protected static int $selfJoinCount = 0;

    /**
     * Create a new relation instance.
     */
    public function __construct(Builder $query, Model $parent)
    {
        $this->query = $query;
        $this->parent = $parent;
        $this->related = $query->getModel();

        $this->addConstraints();
    }

    /**
     * Run a callback with constraints disabled on the relation.
     *
     * @return mixed
     */
    public static function noConstraints(Closure $callback)
    {
        $previous = static::$constraints;

        static::$constraints = false;

        try {
            return $callback();
        } finally {
            static::$constraints = $previous;
        }
    }

    /**
     * Set the base constraints on the relation query.
     *
     * @return void
     */
    abstract public function addConstraints();

    /**
     * Set the constraints for an eager load of the relation.
     *
     * @param  array  $models
     * @return void
     */
    abstract public function addEagerConstraints(array $models);

    /**
     * Initialize the relation on a set of models.
     *
     * @param  array  $models
     * @param  string  $relation
     * @return array
     */
    abstract public function initRelation(array $models, $relation);

    /**
     * Match the eagerly loaded results to their parents.
     *
     * @param  array  $models
     * @param  \Illuminate\Database\Eloquent\Collection  $results
     * @param  string  $relation
     * @return array
     */
    abstract public function match(array $models, Collection $results, $relation);

    /**
     * Get the results of the relationship.
     *
     * @return mixed
     */
    abstract public function getResults();

    /**
     * Get the relationship for eager loading.
     *
     * @return \Illuminate\Database\Eloquent\Collection
     */
    public function getEager()
    {
        return $this->get();
    }

    /**
     * Execute the query as a "select" statement.
     *
     * @param  array  $columns
     * @return \Illuminate\Database\Eloquent\Collection
     */
    public function get($columns = ['*'])
    {
        return $this->query->get($columns);
    }

    /**
     * Touch all of the related models for the relationship.
     *
     * @return void
     */
    public function touch()
    {
        // ClickHouse doesn't support traditional UPDATE for touch
        // This method is left empty intentionally
    }

    /**
     * Run a raw update against the base query.
     *
     * @param  array  $attributes
     * @return int
     */
    public function rawUpdate(array $attributes = [])
    {
        return $this->query->withoutGlobalScopes()->update($attributes);
    }

    /**
     * Get the underlying query for the relation.
     *
     * @return \One23\LaravelClickhouse\Database\Eloquent\Builder
     */
    public function getQuery()
    {
        return $this->query;
    }

    /**
     * Get the base query builder driving the Eloquent builder.
     *
     * @return \One23\LaravelClickhouse\Database\Query\Builder
     */
    public function getBaseQuery()
    {
        return $this->query->getQuery();
    }

    /**
     * Get a base query builder instance.
     *
     * @return \One23\LaravelClickhouse\Database\Query\Builder
     */
    public function toBase()
    {
        return $this->query->toBase();
    }

    /**
     * Get the parent model of the relation.
     *
     * @return \One23\LaravelClickhouse\Database\Eloquent\Model
     */
    public function getParent()
    {
        return $this->parent;
    }

    /**
     * Get the related model of the relation.
     *
     * @return \One23\LaravelClickhouse\Database\Eloquent\Model
     */
    public function getRelated()
    {
        return $this->related;
    }

    /**
     * Get the name of the "created at" column.
     *
     * @return string
     */
    public function createdAt()
    {
        return $this->parent->getCreatedAtColumn();
    }

    /**
     * Get the name of the "updated at" column.
     *
     * @return string
     */
    public function updatedAt()
    {
        return $this->parent->getUpdatedAtColumn();
    }

    /**
     * Get the name of the related model's "updated at" column.
     *
     * @return string
     */
    public function relatedUpdatedAt()
    {
        return $this->related->getUpdatedAtColumn();
    }

    /**
     * Get all of the primary keys for an array of models.
     *
     * @param  array  $models
     * @param  string|null  $key
     * @return array
     */
    protected function getKeys(array $models, $key = null)
    {
        return collect($models)->map(function ($value) use ($key) {
            return $key ? $value->getAttribute($key) : $value->getKey();
        })->values()->unique()->filter(function ($value) {
            return ! is_null($value);
        })->all();
    }

    /**
     * Handle dynamic method calls to the relationship.
     *
     * @param  string  $method
     * @param  array  $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        if (static::hasMacro($method)) {
            return $this->macroCall($method, $parameters);
        }

        return $this->forwardCallTo($this->query, $method, $parameters);
    }

    /**
     * Force a clone of the underlying query builder when cloning.
     *
     * @return void
     */
    public function __clone()
    {
        $this->query = clone $this->query;
    }
}
