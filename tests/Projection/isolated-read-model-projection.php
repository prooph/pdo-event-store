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

use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Pdo\MySqlEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy\MySqlSimpleStreamStrategy;
use Prooph\EventStore\Pdo\Projection\MySqlProjectionManager;
use Prooph\EventStore\Pdo\Projection\PdoEventStoreReadModelProjector;
use Prooph\EventStore\Projection\ReadModel;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Pdo\TestUtil;

require __DIR__ . '/../../vendor/autoload.php';

$readModel = new class() implements ReadModel {
    public function init(): void
    {
    }

    public function isInitialized(): bool
    {
        return true;
    }

    public function reset(): void
    {
    }

    public function delete(): void
    {
    }

    public function stack(string $operation, ...$args): void
    {
    }

    public function persist(): void
    {
    }
};

$connection = TestUtil::getConnection();

$eventStore = new MySqlEventStore(
    new FQCNMessageFactory(),
    $connection,
    new MySqlSimpleStreamStrategy(new NoOpMessageConverter())
);

$projectionManager = new MySqlProjectionManager(
    $eventStore,
    $connection
);
$projection = $projectionManager->createReadModelProjection(
    'test_projection',
    $readModel,
    [
        PdoEventStoreReadModelProjector::OPTION_PCNTL_DISPATCH => true,
        PdoEventStoreReadModelProjector::OPTION_LOCK_TIMEOUT_MS => 3000,
        PdoEventStoreReadModelProjector::OPTION_UPDATE_LOCK_THRESHOLD => 2000,
    ]
);
\pcntl_signal(SIGQUIT, function () use ($projection) {
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
