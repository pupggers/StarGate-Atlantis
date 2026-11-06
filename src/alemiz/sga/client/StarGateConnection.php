<?php

/*
 * Copyright 2020 Alemiz
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

namespace alemiz\sga\client;

use alemiz\sga\codec\ProtocolCodec;
use alemiz\sga\protocol\types\HandshakeData;
use alemiz\sga\utils\StarGateException;
use pmmp\thread\Thread as NativeThread;
use pmmp\thread\ThreadSafeArray;
use pocketmine\thread\log\ThreadSafeLogger;
use pocketmine\thread\Thread;
use pocketmine\utils\Binary;

use function in_array;
use function socket_clear_error;
use function socket_last_error;
use function socket_read;
use function socket_send;
use function socket_strerror;
use function strlen;
use function substr;

use const PHP_BINARY_READ;

class StarGateConnection extends Thread
{
    public const STATE_DISCONNECTED = 0;
    public const STATE_CONNECTING = 1;
    public const STATE_CONNECTED = 2;
    public const STATE_AUTHENTICATING = 3;
    public const STATE_AUTHENTICATED = 4;
    public const STATE_SHUTDOWN = 5;

    private ThreadSafeLogger $logger;
    /** @var StarGateSocket */
    private StarGateSocket $starGateSocket;

    public ?\Socket $socket = null;

    /** @var string */
    private string $address;
    /** @var int */
    private int $port;
    /** @var HandshakeData */
    private HandshakeData $handshakeData;

    private ThreadSafeArray $input;
    private ThreadSafeArray $output;

    /** @var string */
    private string $buffer = '';
    /** Bytes of an outgoing packet the non-blocking socket has not accepted yet. */
    private string $pending = '';

    /** socket_read()/socket_send() codes that only mean "try again": EINTR, EAGAIN (Linux, macOS) and WSAEWOULDBLOCK. */
    private const RETRY_ERRORS = [0, 4, 11, 35, 10035];

    /** @var int */
    private int $state = self::STATE_DISCONNECTED;

    /**
     * StarGateConnection constructor.
     * @param ThreadSafeLogger $logger
     * @param string $address
     * @param int $port
     * @param HandshakeData $handshakeData
     */
    public function __construct(ThreadSafeLogger $logger, string $address, int $port, HandshakeData $handshakeData)
    {
        $this->logger = $logger;
        $this->address = $address;
        $this->port = $port;
        $this->handshakeData = $handshakeData;
        $this->starGateSocket = new StarGateSocket($this, $this->address, $this->port);

        $this->input = new ThreadSafeArray();
        $this->output = new ThreadSafeArray();
        $this->start(NativeThread::INHERIT_NONE);
    }

    public function onRun(): void
    {
        $this->registerClassLoaders();
        gc_enable();
        error_reporting(-1);
        ini_set('display_errors', '1');
        ini_set('display_startup_errors', '1');

        register_shutdown_function([$this, 'shutdown']);
        //set_error_handler([$this, 'errorHandler'], E_ALL);

        $this->state = self::STATE_CONNECTING;
        $this->logger->debug('Connecting to StarGate server '.$this->address);

        if (!$this->starGateSocket->connect()) {
            $this->state = self::STATE_DISCONNECTED;
            return;
        }
        //socket_getpeername($this->socket, $this->address, $this->port);

        $this->state = self::STATE_CONNECTED;
        $this->operate();
    }

    private function operate(): void
    {
        while ($this->state !== self::STATE_DISCONNECTED && $this->state !== self::STATE_SHUTDOWN) {
            $start = microtime(true);
            $this->onTick();
            $time = microtime(true);
            if (($diff = $time - $start) < 0.02) {
                time_sleep_until($time + 0.025 - $diff);
            }
        }
        // Send what is still queued (e.g. our DisconnectPacket on a clean close); a dead peer just fails quietly.
        if ($this->socket !== null) {
            $this->flushOutput($this->socket);
        }
        $this->shutdown();
    }

    private function onTick(): void
    {
        $socket = $this->socket;
        if ($socket === null) {
            $this->lost('no socket');
            return;
        }

        $data = @socket_read($socket, 65536, PHP_BINARY_READ);
        if ($data === '') {
            // A readable socket with nothing to read is end-of-file: the proxy went away.
            $this->lost('the proxy closed the connection');
            return;
        }
        if ($data === false) {
            $error = socket_last_error($socket);
            socket_clear_error($socket);
            if (!in_array($error, self::RETRY_ERRORS, true)) {
                $this->lost(socket_strerror($error));
                return;
            }
        } else {
            $this->buffer .= $data;
        }

        if (!$this->flushOutput($socket)) {
            return;
        }
        try {
            $this->readBuffer();
        } catch (StarGateException $e) {
            $this->lost($e->getMessage());
        }
    }

    /**
     * Writes queued packets without ever raising SIGPIPE: writing to a socket whose peer is gone would otherwise
     * terminate the whole server process (PHP does not ignore SIGPIPE), which is how a proxy outage used to take
     * downstream servers down with no crash dump. Partial writes are kept for the next tick.
     */
    private function flushOutput(\Socket $socket): bool
    {
        $flags = defined('MSG_NOSIGNAL') ? MSG_NOSIGNAL : 0;
        while (true) {
            if ($this->pending === '') {
                $next = $this->outRead();
                if ($next === null || $next === '') {
                    return true;
                }
                $this->pending = $next;
            }
            $sent = @socket_send($socket, $this->pending, strlen($this->pending), $flags);
            if ($sent === false) {
                $error = socket_last_error($socket);
                socket_clear_error($socket);
                if (in_array($error, self::RETRY_ERRORS, true)) {
                    return true;
                }
                $this->pending = '';
                $this->lost(socket_strerror($error));
                return false;
            }
            $this->pending = (string) substr($this->pending, $sent);
            if ($this->pending !== '') {
                return true;
            }
        }
    }

    private function lost(string $reason): void
    {
        if ($this->state === self::STATE_DISCONNECTED || $this->state === self::STATE_SHUTDOWN) {
            return;
        }
        $this->logger->info('§cConnection with StarGate server has disconnected unexpectedly: ' . $reason);
        $this->close();
    }

    private function readBuffer(): void
    {
        if (empty($this->buffer)) {
            return;
        }

        $offset = 0;
        $len = strlen($this->buffer);
        while ($offset < $len) {
            if ($offset > ($len - 6)) {
                // Tried to decode invalid buffer
                break;
            }

            $magic = Binary::readShort(substr($this->buffer, $offset, 2));
            if ($magic !== ProtocolCodec::STARGATE_MAGIC) {
                // A corrupt stream cannot be resynchronised; drop the connection (the client reconnects) instead of killing the thread.
                $this->buffer = '';
                throw new StarGateException('Magic does not match!');
            }
            $offset += 2;

            $length = Binary::readInt(substr($this->buffer, $offset, 4));
            $offset += 4;

            if (($offset + $length) > $len) {
                // Received incomplete packet
                // Retain the complete magic + length header until the payload arrives.
                $offset -= 6;
                break;
            }

            $payload = substr($this->buffer, $offset, $length);
            $offset += $length;
            $this->inputWrite($payload);
        }

        if ($offset < $len) {
            $this->buffer = substr($this->buffer, $offset);
        } else {
            $this->buffer = '';
        }
    }

    /**
     * @param string $payload
     */
    public function writeBuffer(string $payload): void
    {
        $buf = Binary::writeShort(ProtocolCodec::STARGATE_MAGIC);
        $buf .= $payload;
        $this->outWrite($buf);
    }

    public function close(): void
    {
        if ($this->state === self::STATE_DISCONNECTED) {
            return;
        }
        $this->state = self::STATE_DISCONNECTED;
        $this->logger->debug('Closed StarGate session '.$this->address);
    }

    public function shutdown(): void
    {
        if ($this->state === self::STATE_SHUTDOWN) {
            return;
        }
        $this->state = self::STATE_SHUTDOWN;
        $this->starGateSocket->close();
    }

    /**
     * @return bool
     */
    public function isClosed(): bool
    {
        return $this->state === self::STATE_DISCONNECTED || $this->state === self::STATE_SHUTDOWN;
    }

    /**
     * @return int
     */
    public function getState(): int
    {
        return $this->state;
    }

    /**
     * @param int $state
     */
    public function setState(int $state): void
    {
        $this->state = $state;
    }

    /**
     * @return string|null
     */
    public function inputRead(): ?string
    {
        return $this->input->shift();
    }

    /**
     * @param string $string
     */
    public function inputWrite(string $string): void
    {
        $this->input[] = $string;
    }

    /**
     * @return string|null
     */
    public function outRead(): ?string
    {
        return $this->output->shift();
    }

    /**
     * @param string $string
     */
    public function outWrite(string $string): void
    {
        $this->output[] = $string;
    }

    public function quit(): void
    {
        $this->close();
        parent::quit();
    }

    /**
     * @return StarGateSocket
     */
    public function getStarGateSocket(): StarGateSocket
    {
        return $this->starGateSocket;
    }

    public function getSocket(): ?\Socket
    {
        return $this->socket;
    }

    public function getLogger(): ThreadSafeLogger
    {
        return $this->logger;
    }

    public function getClientName(): string
    {
        return $this->handshakeData->getClientName();
    }

    public function getThreadName(): string
    {
        return 'StarGate-Atlantis';
    }

    public function setGarbage(): void
    {
    }
}
