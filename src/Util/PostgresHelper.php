<?php

declare(strict_types=1);

namespace Prooph\EventStore\Pdo\Util;

trait PostgresHelper
{
    /**
     * @param string $tableName
     * @return string
     */
    private function quoteIdent(string $tableName): string
    {
        return array_reduce(explode('.', $tableName), function ($result, $part) {
            return implode('.', array_filter([
                $result,
                '"' . trim($part, '" ') . '"',
            ]));
        }, '');
    }

    /**
     * @param string $identifier
     * @return string|null
     */
    private function extractSchema(string $identifier): ?string
    {
        $parts = array_map(function ($part) {
            return trim($part, '" ');
        }, explode('.', $identifier));

        return count($parts) === 2 ? $parts[0] : null;
    }
}
