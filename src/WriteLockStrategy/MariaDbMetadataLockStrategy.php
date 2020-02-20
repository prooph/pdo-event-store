<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2020 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2016-2020 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Pdo\WriteLockStrategy;

use Prooph\EventStore\Pdo\WriteLockStrategy;

final class MariaDbMetadataLockStrategy implements WriteLockStrategy
{
    /**
     * @var \PDO
     */
    private $connection;

    /**
     * @var int
     */
    private $timeout;

    public function __construct(\PDO $connection, int $timeout = 0xffffff)
    {
        if ($timeout < 0) {
            throw new \InvalidArgumentException('$timeout must be greater or equal to 0.');
        }

        $this->connection = $connection;
        $this->timeout = $timeout;
    }

    public function getLock(string $name): bool
    {
        try {
            $res = $this->connection->query('SELECT GET_LOCK(\'' . $name . '\', ' . $this->timeout . ') as \'get_lock\'');
        } catch (\PDOException $e) {
            // ER_USER_LOCK_DEADLOCK: we only care for deadlock errors and fail locking
            if ('3058' === $this->connection->errorCode()) {
                return false;
            }

            throw $e;
        }

        if (! $res) {
            return false;
        }

        $lockStatus = $res->fetchAll();
        if ('1' === $lockStatus[0]['get_lock']) {
            return true;
        }

        return false;
    }

    public function releaseLock(string $name): bool
    {
        $res = $this->connection->query('SELECT RELEASE_LOCK(\'' . $name . '\') as \'release_lock\'');

        if ($res) {
            $res->fetchAll();
        }

        return true;
    }
}
