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

namespace Prooph\EventStore\Pdo;

interface MariaDBIndexedPersistenceStrategy
{
    /**
     * Return an array of indexed columns to enable the use of indexes in MariaDB
     *
     * @example
     *      [
     *          '_aggregate_id' => 'aggregate_id',
     *          '_aggregate_type' => 'aggregate_type',
     *          '_aggregate_version' => 'aggregate_version',
     *      ]
     */
    public function indexedMetadataFields(): array;
}
