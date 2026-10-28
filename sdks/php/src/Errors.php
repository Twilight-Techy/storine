<?php

declare(strict_types=1);

namespace Storine;

// Every error the client raises. Each says what happened and what to do about it.
// (One file: the autoloader loads it with StorineException.)

/** Base class of every Storine error. */
class StorineException extends \RuntimeException
{
    public function __construct(string $message, public readonly ?int $status = null)
    {
        parent::__construct($message);
    }
}

/** The document does not exist (e.g. `update` of a missing key). */
class NotFoundException extends StorineException
{
    public ?int $writeIndex = null;
}

/**
 * A condition did not hold: the document already exists, does not exist, or changed.
 * `writeIndex` is the position of the failing write in the request and `currentVersion` the
 * document's version now (null if it does not exist). Re-read and decide again.
 */
class ConflictException extends StorineException
{
    public function __construct(string $message, public readonly ?int $writeIndex = null, public readonly ?int $currentVersion = null)
    {
        parent::__construct($message, 0x04);
    }
}

/** The request was rejected as invalid (bad value, type mismatch, bound violated). Do not retry. */
class InvalidException extends StorineException
{
}

/** Authentication failed or the token's role does not allow this operation. */
class UnauthorizedException extends StorineException
{
}

/** The server is temporarily overloaded. Retry later. */
class OverloadedException extends StorineException
{
}

/** This connection exceeded its rate limit. Retry later. */
class RateLimitedException extends OverloadedException
{
}

/** No server (or no cluster leader) could be reached in time. Retry later. */
class UnavailableException extends StorineException
{
}

/** The server failed internally (I/O error...). The message has the details. */
class ServerException extends StorineException
{
}
