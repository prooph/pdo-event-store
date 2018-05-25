<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2018 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2018 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
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
        $parts = array_filter($this->splitIdent($ident));
        $parts = array_map(function ($ident) {
            return '"' . trim($ident, '" ') . '"';
        }, $parts);

        return implode('.', $parts);
    }

    /**
     * Splits fully qualified table name by first dot.
     * @param string $ident
     * @return string[]
     */
    private function splitIdent(string $ident): array
    {
        if (false === ($pos = strpos($ident, '.'))) {
            return ['', $ident];
        }

        $schema = substr($ident, 0, $pos);
        $ident = substr($ident, $pos + 1);

        return [$schema, $ident];
    }
}
