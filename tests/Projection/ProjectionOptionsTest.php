<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2016 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2016 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\PDO\Projection;

use PHPUnit\Framework\TestCase;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\PDO\Projection\ProjectionOptions;

class ProjectionOptionsTest extends TestCase
{
    /**
     * @test
     */
    public function it_throws_exception_when_cache_size_option_is_missing(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ProjectionOptions::fromArray([]);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_persist_blocksize_option_is_missing(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ProjectionOptions::fromArray(['cache_size' => 1]);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_projections_table_option_is_missing(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ProjectionOptions::fromArray([
            'cache_size' => 1,
            'persist_block_size' => 2,
        ]);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_lock_timeout_ms_option_is_missing(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ProjectionOptions::fromArray([
            'cache_size' => 1,
            'persist_block_size' => 2,
            'projections_table' => 'foo',
        ]);
    }

    /**
     * @test
     */
    public function it_creates_instance(): void
    {
        $options = ProjectionOptions::fromArray([
            'cache_size' => 5,
            'persist_block_size' => 15,
            'projections_table' => 'foo',
            'lock_timeout_ms' => 100,
        ]);

        $this->assertEquals(5, $options->cacheSize());
        $this->assertEquals(15, $options->persistBlockSize());
        $this->assertEquals('foo', $options->projectionsTable());
        $this->assertEquals(100, $options->lockTimeoutMs());
    }
}
