<?php

use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\Pdo\MySqlEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy\MySqlSimpleStreamStrategy;
use Prooph\EventStore\Pdo\Projection\MySqlProjectionManager;
use Prooph\EventStore\Pdo\Projection\PdoEventStoreProjector;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Pdo\TestUtil;

require __DIR__ . '/../../vendor/autoload.php';

$connection = TestUtil::getConnection();
TestUtil::initDefaultDatabaseTables($connection);

$eventStore = new MySqlEventStore(
    new FQCNMessageFactory(),
    $connection,
    new MySqlSimpleStreamStrategy()
);

$projectionManager = new MySqlProjectionManager(
    $eventStore,
    $connection
);
$projection = $projectionManager->createProjection(
    'test_projection',
    [
        PdoEventStoreProjector::OPTION_PCNTL_DISPATCH => true
    ]
);
pcntl_signal(SIGQUIT, function () use ($projection) {
    $projection->stop();
    exit(SIGUSR1);
});
$projection
    ->fromStream('user-123')
    ->when([
        UserCreated::class => function (array $state, UserCreated $event): array {
            return $state;
        },
    ])
    ->run();
