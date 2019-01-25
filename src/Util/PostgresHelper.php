<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2019 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2019 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Pdo\Util;

/**
 * PostgreSQL helper to work with fully qualified table name.
 */
trait PostgresHelper
{
    /**
     * @param string $ident
     * @return string
     */
    private function quoteIdent(string $ident): string
    {
        $pos = \strpos($ident, '.');

        if (false === $pos) {
            return '"' . $ident . '"';
        }

        $schema = \substr($ident, 0, $pos);
        $table = \substr($ident, $pos + 1);

        return '"' . $schema . '"."' . $table . '"';
    }

    /**
     * Extracts schema name as string before the first dot.
     * @param string $ident
     * @return string|null
     */
    private function extractSchema(string $ident): ?string
    {
        if (false === ($pos = \strpos($ident, '.'))) {
            return null;
        }

        return \substr($ident, 0, $pos);
    }
}
