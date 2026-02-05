<?php

declare(strict_types=1);

namespace One23\LaravelClickhouse\Database\Eloquent;

use Illuminate\Database\Eloquent\Scope;

/**
 * Soft Deleting Scope for ClickHouse.
 *
 * Filters out soft-deleted records by checking is_deleted = 0.
 */
class SoftDeletingScope implements Scope
{
    /**
     * All of the extensions to be added to the builder.
     *
     * @var string[]
     */
    protected array $extensions = [
        'Restore',
        'RestoreOrCreate',
        'CreateOrRestore',
        'WithTrashed',
        'WithoutTrashed',
        'OnlyTrashed',
    ];

    /**
     * Apply the scope to a given Eloquent query builder.
     *
     * @param  \Illuminate\Database\Eloquent\Builder  $builder
     * @param  \Illuminate\Database\Eloquent\Model  $model
     * @return void
     */
    public function apply($builder, $model): void
    {
        $builder->where($model->getQualifiedDeletedFlagColumn(), 0);
    }

    /**
     * Extend the query builder with the needed functions.
     *
     * @param  \One23\LaravelClickhouse\Database\Eloquent\Builder  $builder
     * @return void
     */
    public function extend(Builder $builder): void
    {
        foreach ($this->extensions as $extension) {
            $this->{"add{$extension}"}($builder);
        }

        $builder->onDelete(function (Builder $builder) {
            $column = $this->getDeletedFlagColumn($builder);

            // For soft delete, we update the is_deleted flag to 1
            return $builder->update([
                $column => 1,
                $this->getDeletedAtColumn($builder) => $builder->getModel()->freshTimestampString(),
            ]);
        });
    }

    /**
     * Get the "deleted flag" column for the builder.
     *
     * @param  \One23\LaravelClickhouse\Database\Eloquent\Builder  $builder
     * @return string
     */
    protected function getDeletedFlagColumn(Builder $builder): string
    {
        if (count($builder->getQuery()->joins ?? []) > 0) {
            return $builder->getModel()->getQualifiedDeletedFlagColumn();
        }

        return $builder->getModel()->getDeletedFlagColumn();
    }

    /**
     * Get the "deleted at" column for the builder.
     *
     * @param  \One23\LaravelClickhouse\Database\Eloquent\Builder  $builder
     * @return string
     */
    protected function getDeletedAtColumn(Builder $builder): string
    {
        if (count($builder->getQuery()->joins ?? []) > 0) {
            return $builder->getModel()->getQualifiedDeletedAtColumn();
        }

        return $builder->getModel()->getDeletedAtColumn();
    }

    /**
     * Add the restore extension to the builder.
     *
     * @param  \One23\LaravelClickhouse\Database\Eloquent\Builder  $builder
     * @return void
     */
    protected function addRestore(Builder $builder): void
    {
        $builder->macro('restore', function (Builder $builder) {
            $builder->withTrashed();

            return $builder->update([
                $builder->getModel()->getDeletedFlagColumn() => 0,
                $builder->getModel()->getDeletedAtColumn() => null,
            ]);
        });
    }

    /**
     * Add the restore-or-create extension to the builder.
     *
     * @param  \One23\LaravelClickhouse\Database\Eloquent\Builder  $builder
     * @return void
     */
    protected function addRestoreOrCreate(Builder $builder): void
    {
        $builder->macro('restoreOrCreate', function (Builder $builder, array $attributes = [], array $values = []) {
            $builder->withTrashed();

            return tap($builder->firstOrNew($attributes, $values), function ($instance) {
                if ($instance->exists && $instance->trashed()) {
                    $instance->restore();
                } elseif (! $instance->exists) {
                    $instance->save();
                }
            });
        });
    }

    /**
     * Add the create-or-restore extension to the builder.
     *
     * @param  \One23\LaravelClickhouse\Database\Eloquent\Builder  $builder
     * @return void
     */
    protected function addCreateOrRestore(Builder $builder): void
    {
        $builder->macro('createOrRestore', function (Builder $builder, array $attributes = [], array $values = []) {
            return $builder->restoreOrCreate($attributes, $values);
        });
    }

    /**
     * Add the with-trashed extension to the builder.
     *
     * @param  \One23\LaravelClickhouse\Database\Eloquent\Builder  $builder
     * @return void
     */
    protected function addWithTrashed(Builder $builder): void
    {
        $builder->macro('withTrashed', function (Builder $builder, $withTrashed = true) {
            if (! $withTrashed) {
                return $builder->withoutTrashed();
            }

            return $builder->withoutGlobalScope($this);
        });
    }

    /**
     * Add the without-trashed extension to the builder.
     *
     * @param  \One23\LaravelClickhouse\Database\Eloquent\Builder  $builder
     * @return void
     */
    protected function addWithoutTrashed(Builder $builder): void
    {
        $builder->macro('withoutTrashed', function (Builder $builder) {
            $model = $builder->getModel();

            $builder->withoutGlobalScope($this)->where(
                $model->getQualifiedDeletedFlagColumn(),
                0
            );

            return $builder;
        });
    }

    /**
     * Add the only-trashed extension to the builder.
     *
     * @param  \One23\LaravelClickhouse\Database\Eloquent\Builder  $builder
     * @return void
     */
    protected function addOnlyTrashed(Builder $builder): void
    {
        $builder->macro('onlyTrashed', function (Builder $builder) {
            $model = $builder->getModel();

            $builder->withoutGlobalScope($this)->where(
                $model->getQualifiedDeletedFlagColumn(),
                1
            );

            return $builder;
        });
    }
}
