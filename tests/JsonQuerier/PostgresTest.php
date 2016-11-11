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

namespace ProophTest\EventStore\PDO;

use Prooph\EventStore\PDO\JsonQuerier\Postgres;
use PHPUnit_Framework_TestCase as TestCase;

/**
 * @covers \Prooph\EventStore\PDO\JsonQuerier\Postgres
 */
final class PostgresTest extends TestCase
{
    /**
     * @test
     */
    public function it_should_select_data_from_metadata()
    {
        $sut = new Postgres();
        $this->assertSame('metadata->>\'metadata_data\'', $sut->metadata('metadata_data'));
    }

    /**
     * @test
     */
    public function it_should_select_data_from_payload()
    {
        $sut = new Postgres();
        $this->assertSame('payload->>\'payload_data\'', $sut->payload('payload_data'));
    }
}
