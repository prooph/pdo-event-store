<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2022 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2016-2022 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Pdo\PersistenceStrategy\PostgresSimpleStreamStrategy;
use Prooph\EventStore\Pdo\PostgresEventStore;
use Prooph\EventStore\Pdo\Projection\PostgresProjectionManager;
use Prooph\EventStore\Projection\Query;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\TestDomainEvent;
use ProophTest\EventStore\Pdo\TestUtil;

require __DIR__ . '/../../vendor/autoload.php';

$connection = TestUtil::getConnection();

$eventStore = new PostgresEventStore(
    new FQCNMessageFactory(),
    $connection,
    new PostgresSimpleStreamStrategy(new NoOpMessageConverter())
);
$events = [];

for ($i = 0; $i < 100; $i++) {
    $events[] = TestDomainEvent::with(['test' => 1], $i);
    $i++;
}

$eventStore->create(new Stream(new StreamName('user-123'), new ArrayIterator($events)));

$projectionManager = new PostgresProjectionManager(
    $eventStore,
    $connection
);

$query = $projectionManager->createQuery(
    [
        Query::OPTION_PCNTL_DISPATCH => true,
    ]
);

\pcntl_signal(SIGQUIT, function () use ($query) {
    $query->stop();
    exit(SIGUSR1);
});

$query
    ->fromStreams('user-123')
    ->whenAny(function () {
        \usleep(500000);
    })
    ->run();
