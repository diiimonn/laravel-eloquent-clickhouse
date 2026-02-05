<?php

declare(strict_types=1);

namespace One23\LaravelClickhouse\Database\Eloquent;

/**
 * Soft Deletes for ClickHouse using ReplacingMergeTree pattern.
 *
 * ClickHouse doesn't support traditional UPDATE/DELETE operations efficiently.
 * Instead, this trait uses the ReplacingMergeTree engine pattern:
 * - Inserts a new row with is_deleted = 1 to mark as deleted
 * - Uses WHERE is_deleted = 0 to filter out deleted records
 *
 * Table requirements:
 * - Use ReplacingMergeTree engine with version and is_deleted columns
 * - Add uInt8('is_deleted')->default(0) column
 * - Add dateTime('deleted_at')->nullable() column
 * - Configure: SETTINGS clean_deleted_rows = 'Always'
 *
 * Example migration:
 * ```php
 * $table->uInt8('is_deleted')->default(0);
 * $table->dateTime('deleted_at')->nullable();
 * $table->replacingMergeTree('version', 'is_deleted');
 * $table->setting('clean_deleted_rows', 'Always');
 * ```
 */
trait SoftDeletes
{
    /**
     * Indicates if the model is currently force deleting.
     */
    protected bool $forceDeleting = false;

    /**
     * Boot the soft deleting trait for a model.
     */
    public static function bootSoftDeletes(): void
    {
        static::addGlobalScope(new SoftDeletingScope());
    }

    /**
     * Initialize the soft deleting trait for an instance.
     */
    public function initializeSoftDeletes(): void
    {
        if (! isset($this->casts[$this->getDeletedAtColumn()])) {
            $this->casts[$this->getDeletedAtColumn()] = 'datetime';
        }
    }

    /**
     * Force a hard delete on a soft deleted model.
     *
     * @return bool|null
     */
    public function forceDelete()
    {
        $this->forceDeleting = true;

        return tap($this->delete(), function ($deleted) {
            $this->forceDeleting = false;

            if ($deleted) {
                $this->fireModelEvent('forceDeleted', false);
            }
        });
    }

    /**
     * Perform the actual delete query on this model instance.
     *
     * In ClickHouse, soft delete inserts a new row with is_deleted = 1.
     */
    protected function performDeleteOnModel(): void
    {
        if ($this->forceDeleting) {
            $this->newModelQuery()
                ->where($this->getKeyName(), $this->getKey())
                ->forceDelete();

            $this->exists = false;

            return;
        }

        $this->runSoftDelete();
    }

    /**
     * Perform the actual soft delete.
     *
     * For ClickHouse, we insert a new row with is_deleted = 1.
     * ReplacingMergeTree will handle deduplication.
     */
    protected function runSoftDelete(): void
    {
        $query = $this->newModelQuery();

        $time = $this->freshTimestamp();

        $columns = [
            $this->getDeletedFlagColumn() => 1,
            $this->getDeletedAtColumn() => $time,
        ];

        $this->{$this->getDeletedFlagColumn()} = 1;
        $this->{$this->getDeletedAtColumn()} = $time;

        // Get all current attributes for the insert
        $attributes = $this->getAttributes();

        // Update the soft delete columns
        $attributes[$this->getDeletedFlagColumn()] = 1;
        $attributes[$this->getDeletedAtColumn()] = $this->fromDateTime($time);

        // In ClickHouse, we insert a new row instead of updating
        // ReplacingMergeTree will deduplicate based on ORDER BY columns
        $query->insert($attributes);

        $this->syncOriginalAttribute($this->getDeletedFlagColumn());
        $this->syncOriginalAttribute($this->getDeletedAtColumn());

        $this->fireModelEvent('trashed', false);
    }

    /**
     * Restore a soft-deleted model instance.
     *
     * For ClickHouse, we insert a new row with is_deleted = 0.
     *
     * @return bool
     */
    public function restore(): bool
    {
        // If the restoring event does not return false, we will proceed with this
        // restore operation. Otherwise, we bail out so the developer will stop
        // the restore totally. We will clear the deleted timestamp and save.
        if ($this->fireModelEvent('restoring') === false) {
            return false;
        }

        $this->{$this->getDeletedFlagColumn()} = 0;
        $this->{$this->getDeletedAtColumn()} = null;

        // Get all current attributes for the insert
        $attributes = $this->getAttributes();

        // Update the soft delete columns
        $attributes[$this->getDeletedFlagColumn()] = 0;
        $attributes[$this->getDeletedAtColumn()] = null;

        // Insert a new row with is_deleted = 0
        $this->newModelQuery()->insert($attributes);

        $this->exists = true;

        $this->fireModelEvent('restored', false);

        return true;
    }

    /**
     * Restore a soft-deleted model instance without raising any events.
     *
     * @return bool
     */
    public function restoreQuietly(): bool
    {
        return static::withoutEvents(fn () => $this->restore());
    }

    /**
     * Determine if the model instance has been soft-deleted.
     *
     * @return bool
     */
    public function trashed(): bool
    {
        return (int) $this->{$this->getDeletedFlagColumn()} === 1;
    }

    /**
     * Register a "softDeleted" model event callback with the dispatcher.
     */
    public static function softDeleted(callable $callback): void
    {
        static::registerModelEvent('trashed', $callback);
    }

    /**
     * Register a "restoring" model event callback with the dispatcher.
     */
    public static function restoring(callable $callback): void
    {
        static::registerModelEvent('restoring', $callback);
    }

    /**
     * Register a "restored" model event callback with the dispatcher.
     */
    public static function restored(callable $callback): void
    {
        static::registerModelEvent('restored', $callback);
    }

    /**
     * Register a "forceDeleting" model event callback with the dispatcher.
     */
    public static function forceDeleting(callable $callback): void
    {
        static::registerModelEvent('forceDeleting', $callback);
    }

    /**
     * Register a "forceDeleted" model event callback with the dispatcher.
     */
    public static function forceDeleted(callable $callback): void
    {
        static::registerModelEvent('forceDeleted', $callback);
    }

    /**
     * Determine if the model is currently force deleting.
     *
     * @return bool
     */
    public function isForceDeleting(): bool
    {
        return $this->forceDeleting;
    }

    /**
     * Get the name of the "deleted flag" column.
     *
     * @return string
     */
    public function getDeletedFlagColumn(): string
    {
        return defined(static::class.'::DELETED_FLAG')
            ? static::DELETED_FLAG
            : 'is_deleted';
    }

    /**
     * Get the name of the "deleted at" column.
     *
     * @return string
     */
    public function getDeletedAtColumn(): string
    {
        return defined(static::class.'::DELETED_AT')
            ? static::DELETED_AT
            : 'deleted_at';
    }

    /**
     * Get the fully qualified "deleted flag" column.
     *
     * @return string
     */
    public function getQualifiedDeletedFlagColumn(): string
    {
        return $this->qualifyColumn($this->getDeletedFlagColumn());
    }

    /**
     * Get the fully qualified "deleted at" column.
     *
     * @return string
     */
    public function getQualifiedDeletedAtColumn(): string
    {
        return $this->qualifyColumn($this->getDeletedAtColumn());
    }
}
