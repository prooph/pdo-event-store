<?php declare(strict_types = 1);

namespace Prooph\EventStore\Pdo\Exception;

use Prooph\EventStore\Exception\ConcurrencyException;

class ConcurrencyExceptionFactory
{
    public static function fromStatementErrorInfo(array $errorInfo): ConcurrencyException
    {
        return new ConcurrencyException(
            sprintf(
                "Some of the aggregate IDs or event IDs have already been used in the same stream. The PDO error should contain more information:\nError %s.\nError-Info: %s",
                $errorInfo[0],
                $errorInfo[2]
            )
        );
    }
}
