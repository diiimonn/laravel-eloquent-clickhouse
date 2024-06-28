<?php

namespace Tests;

class TestCase extends \PHPUnit\Framework\TestCase
{
    protected function assertException(
        \Closure $closure,
        ?string $exception = null
    ): void {
        try {
            $closure();
            $this->assertTrue(false);
        } catch (\Throwable $e) {
            if (
                $exception &&
                $exception !== get_class($e)
            ) {
                $this->assertTrue(false);
            }

            $this->assertTrue(true);
        }
    }
}
