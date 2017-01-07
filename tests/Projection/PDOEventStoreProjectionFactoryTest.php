<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2017 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2017 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\PDO\Projection;

use PDO;
use PHPUnit\Framework\TestCase;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\PDO\Projection\PDOEventStoreProjectionFactory;
use Prooph\EventStore\Projection\ProjectionOptions;

class PDOEventStoreProjectionFactoryTest extends TestCase
{
    /**
     * @test
     */
    public function it_throws_exception_when_invalid_projection_options_given(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $factory = new PDOEventStoreProjectionFactory($this->prophesize(PDO::class)->reveal(), 'test_projection');

        $factory(
            $this->prophesize(EventStore::class)->reveal(),
            'test',
            $this->prophesize(ProjectionOptions::class)->reveal()
        );
    }
}
