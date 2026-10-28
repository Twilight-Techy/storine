using System;

namespace Storine;

/// <summary>Base class of every Storine error. Each says what happened and what to do about it.</summary>
public class StorineException : Exception
{
    /// <summary>The wire status, or -1 when the error was detected locally.</summary>
    public int Status { get; }

    /// <summary>An error with a message and wire status.</summary>
    public StorineException(string message, int status = -1) : base(message) => Status = status;
}

/// <summary>The document does not exist (e.g. <c>UpdateAsync</c> of a missing key).</summary>
public class NotFoundException : StorineException
{
    /// <summary>Position of the write (in a transaction) whose document was missing, or -1.</summary>
    public int WriteIndex { get; }

    /// <summary>A not-found error.</summary>
    public NotFoundException(string message, int status = -1, int writeIndex = -1) : base(message, status) => WriteIndex = writeIndex;
}

/// <summary>A condition did not hold: the document already exists, does not exist, or changed. Re-read and decide again.</summary>
public class ConflictException : StorineException
{
    /// <summary>Position of the failing write in the request, or -1 if unknown.</summary>
    public int WriteIndex { get; }

    /// <summary>The document's version now, or null if it does not exist.</summary>
    public ulong? CurrentVersion { get; }

    /// <summary>A conflict error.</summary>
    public ConflictException(string message, int writeIndex, ulong? currentVersion) : base(message, 4)
    {
        WriteIndex = writeIndex;
        CurrentVersion = currentVersion;
    }
}

/// <summary>The request was rejected as invalid (bad value, type mismatch, bound violated). Do not retry.</summary>
public class InvalidException : StorineException
{
    /// <summary>An invalid-request error.</summary>
    public InvalidException(string message, int status = -1) : base(message, status) { }
}

/// <summary>Authentication failed or the token's role does not allow this operation.</summary>
public class UnauthorizedException : StorineException
{
    /// <summary>An authorization error.</summary>
    public UnauthorizedException(string message, int status = -1) : base(message, status) { }
}

/// <summary>The server is temporarily overloaded. Retry later.</summary>
public class OverloadedException : StorineException
{
    /// <summary>An overload error.</summary>
    public OverloadedException(string message, int status = -1) : base(message, status) { }
}

/// <summary>This connection exceeded its rate limit. Retry later.</summary>
public class RateLimitedException : OverloadedException
{
    /// <summary>A rate-limit error.</summary>
    public RateLimitedException(string message, int status = -1) : base(message, status) { }
}

/// <summary>No server (or no cluster leader) could be reached in time. Retry later.</summary>
public class UnavailableException : StorineException
{
    /// <summary>An availability error.</summary>
    public UnavailableException(string message, int status = -1) : base(message, status) { }
}

/// <summary>The server failed internally (I/O error...). The message has the details.</summary>
public class ServerException : StorineException
{
    /// <summary>A server error.</summary>
    public ServerException(string message, int status = -1) : base(message, status) { }
}
