<?php

declare(strict_types=1);

namespace Storine;

/** A connection failed or timed out (retried by the client; surfaces as UnavailableException). @internal */
final class TransportException extends \RuntimeException
{
}
