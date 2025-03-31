<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2025 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2016-2025 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\Pdo\PersistenceStrategy;

use PHPUnit\Framework\TestCase;
use Prooph\EventStore\Pdo\PersistenceStrategy;
use Prooph\EventStore\StreamName;

abstract class AbstractPostgresPersistenceStrategyTestCase extends TestCase
{
    /** @var PersistenceStrategy */
    protected $strategy;

    protected function setUp(): void
    {
        $this->strategy = $this->createStrategy();
    }

    abstract protected function createStrategy(): PersistenceStrategy;

    /**
     * @test
     */
    public function it_generates_table_name_without_schema_from_stream_name_without_dot(): void
    {
        $this->assertEquals('_' . \sha1('foo'), $this->strategy->generateTableName(new StreamName('foo')));
        $this->assertEquals('_' . \sha1('Prooph\User'), $this->strategy->generateTableName(new StreamName('Prooph\User')));
    }

    /**
     * @test
     */
    public function it_generates_table_name_with_custom_schema_from_stream_name_with_dot(): void
    {
        $this->assertEquals('foo._' . \sha1('foo.bar'), $this->strategy->generateTableName(new StreamName('foo.bar')));
        $this->assertEquals('prooph._' . \sha1('prooph.Prooph\User'), $this->strategy->generateTableName(new StreamName('prooph.Prooph\User')));
    }
}
