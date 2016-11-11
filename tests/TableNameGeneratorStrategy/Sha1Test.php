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

namespace ProophTest\EventStore\PDO\TableNameGeneratorStrategy;

use PHPUnit_Framework_TestCase as TestCase;
use Prooph\EventStore\PDO\TableNameGeneratorStrategy\Sha1;
use Prooph\EventStore\StreamName;

/**
 * @covers \Prooph\EventStore\PDO\TableNameGeneratorStrategy\Sha1
 */
final class Sha1Test extends TestCase
{
    /**
     * @test
     */
    public function it_should_sha1_stream_name(): void
    {
        self::assertSame(
            '_df4e1fe47cf415fe345ca7dd7f5419c60c83d6ce',
            (new Sha1())->__invoke(new StreamName('stream-name'))
        );
    }
}
