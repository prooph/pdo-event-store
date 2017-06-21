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

namespace ProophTest\EventStore\PersistenceStrategy;

use ArrayIterator;
use Prooph\Common\Messaging\DomainEvent;
use Prooph\Common\Messaging\PayloadTrait;
use PHPUnit\Framework\TestCase;
use Prooph\EventStore\Pdo\PersistenceStrategy\MySqlSimpleStreamStrategy;

final class MySqlSimpleStreamStrategyTest extends TestCase
{
    /**
     * @test
     */
    public function it_can_serialise_and_unserialise_payloads_without_malforming_lists(): void
    {
        $stategy = new MySqlSimpleStreamStrategy();

        $expected = [
            'my_list' => [
                'item_one',
                'item_two',
                'item_three',
                'item_four',
            ]
        ];

        $events = new ArrayIterator([
            new class($expected) extends DomainEvent {
                use PayloadTrait;
            }
        ]);

        $out = $stategy->prepareData($events);

        $this->assertEquals($expected, json_decode($out[2], true));
    }
}
