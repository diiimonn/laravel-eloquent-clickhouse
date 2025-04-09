<?php

declare(strict_types=1);

namespace One23\LaravelClickhouse\Database\Eloquent;

use Illuminate\Database\Eloquent\Model as BaseModel;
use Illuminate\Support\Str;

/**
 * if use extends Illuminate\Database\Eloquent\Model we have errors =(
 */
abstract class Model extends BaseModel
{
    protected $connection = 'clickhouse';

    /** Indicates if the IDs are auto-incrementing. */
    public $incrementing = false;

    protected static string $builder = Builder::class;

    public function __construct(array $attributes = [])
    {
        $this->incrementing = false;

        parent::__construct($attributes);
    }

    protected function fillableFromArray(array $attributes)
    {
        $fillable = parent::fillableFromArray($attributes);

        $res = [];
        foreach ($fillable as $key => $value) {
            $key = $this->removeTableFromKey($key);

            $res[$key] = $value;
        }

        return $res;
    }

    /**
     * Get a new query builder that doesn't have any global scopes.
     *
     * @return \Illuminate\Database\Eloquent\Builder<static>
     */
    public function newQueryWithoutScopes()
    {
        return $this->newModelQuery()
            ->with($this->with);
        //    ->withCount($this->withCount)
    }

    /**
     * Set whether IDs are incrementing.
     *
     * @param  bool  $value
     * @return $this
     */
    public function setIncrementing($value)
    {
        throw new \Exception('Is not supported on Clickhouse models.');
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

        return parent::__call($method, $parameters);
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
