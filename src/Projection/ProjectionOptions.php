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

namespace Prooph\EventStore\PDO\Projection;

use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\Projection\ProjectionOptions as BaseProjectionOptions;

class ProjectionOptions extends BaseProjectionOptions
{
    /**
     * @var string
     */
    protected $projectionsTable;

    /**
     * @var int
     */
    protected $lockTimeoutMs;

    public function __construct(
        string $projectionsTable = 'projections',
        int $cacheSize = 1000,
        int $persistBlockSize = 1000,
        int $lockTimeoutMs = 1000
    ) {
        parent::__construct($cacheSize, $persistBlockSize);

        $this->projectionsTable = $projectionsTable;
        $this->lockTimeoutMs = $lockTimeoutMs;
    }

    public static function fromArray(array $data): BaseProjectionOptions
    {
        self::validateData($data);

        return new self(
            $data['projections_table'],
            $data['cache_size'],
            $data['persist_block_size'],
            $data['lock_timeout_ms']
        );
    }

    public function lockTimeoutMs(): int
    {
        return $this->lockTimeoutMs;
    }

    public function projectionsTable(): string
    {
        return $this->projectionsTable;
    }

    /**
     * @throws InvalidArgumentException
     */
    protected static function validateData(array $data): void
    {
        parent::validateData($data);

        if (! isset($data['projections_table'])) {
            throw new InvalidArgumentException('projections_table option missing');
        }

        if (! isset($data['lock_timeout_ms'])) {
            throw new InvalidArgumentException('lock_timeout_ms option missing');
        }
    }
}
