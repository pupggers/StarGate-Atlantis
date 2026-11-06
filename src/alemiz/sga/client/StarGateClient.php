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
use alemiz\sga\events\ClientAuthenticatedEvent;
use alemiz\sga\events\ClientConnectedEvent;
use alemiz\sga\events\ClientDisconnectedEvent;
use alemiz\sga\handler\SessionHandler;
use alemiz\sga\protocol\DisconnectPacket;
use alemiz\sga\protocol\StarGatePacket;
use alemiz\sga\protocol\types\HandshakeData;
use alemiz\sga\StarGateAtlantis;
use alemiz\sga\utils\PacketResponse;
use pocketmine\scheduler\Task;
use pocketmine\Server;

class StarGateClient extends Task
{
    /** @var StarGateAtlantis */
    private StarGateAtlantis $loader;
    /** @var Server */
    private Server $server;
    private \AttachableLogger $logger;

    /** @var ProtocolCodec */
    private ProtocolCodec $protocolCodec;

    /** @var HandshakeData */
    private HandshakeData $handshakeData;
    /** @var string */
    protected string $address;
    /** @var int */
    protected int $port;

    /** @var ClientSession|null */
    private ?ClientSession $session = null;

    /** @var SessionHandler|null */
    private ?SessionHandler $customHandler = null;

    /** Reconnect backoff, seconds: the first retry is quick, then it doubles up to RETRY_MAX while the proxy stays down. */
    private const RETRY_MIN = 2.0;
    private const RETRY_MAX = 30.0;

    /** connect() has been called at least once, so a lost or failed connection is retried. */
    private bool $active = false;
    /** shutdown() was called: never reconnect again. */
    private bool $stopping = false;
    private bool $wasConnected = false;
    private bool $outageLogged = false;
    private float $retryDelay = self::RETRY_MIN;
    private float $nextAttempt = 0.0;

    /**
     * StarGateClient constructor.
     * @param string $address
     * @param int $port
     * @param HandshakeData $handshakeData
     * @param StarGateAtlantis $plugin
     */
    public function __construct(string $address, int $port, HandshakeData $handshakeData, StarGateAtlantis $plugin)
    {
        $this->loader = $plugin;
        $this->server = $plugin->getServer();
        $this->logger = $plugin->getLogger();
        $this->protocolCodec = new ProtocolCodec();

        $this->address = $address;
        $this->port = $port;
        $this->handshakeData = $handshakeData;
        $this->loader->getScheduler()->scheduleDelayedRepeatingTask($this, 20, $this->loader->getTickInterval());
    }

    /**
     * Starts a connection attempt. Safe to call repeatedly: it does nothing while connected or while a previous
     * attempt's thread is still trying, and it joins finished threads so retries never pile threads up.
     */
    public function connect(): void
    {
        if ($this->stopping || $this->isConnected()) {
            return;
        }
        $this->active = true;
        if ($this->session !== null) {
            $previous = $this->session->getConnection();
            if ($previous->isRunning()) {
                return;
            }
            $previous->quit();
        }
        $this->nextAttempt = microtime(true) + $this->retryDelay;
        $this->retryDelay = min(self::RETRY_MAX, $this->retryDelay * 2);
        $this->session = new ClientSession($this, $this->address, $this->port);
    }

    public function onRun(): void
    {
        if ($this->session !== null && $this->isConnected()) {
            $this->session->onTick();
        }
        if ($this->stopping || !$this->active || $this->isConnected()) {
            return;
        }
        // Lost without a DisconnectPacket (the proxy died or the network dropped): tell listeners once.
        if ($this->wasConnected) {
            $this->onSessionDisconnected();
        }
        if (!$this->outageLogged) {
            $this->outageLogged = true;
            $this->logger->warning('§cStarGate proxy ' . $this->address . ':' . $this->port . ' is unreachable; retrying until it is back.');
        }
        if (microtime(true) >= $this->nextAttempt) {
            $this->connect();
        }
    }

    public function onSessionConnected(): void
    {
        $this->retryDelay = self::RETRY_MIN;
        if ($this->outageLogged) {
            $this->outageLogged = false;
            $this->logger->info('§bStarGate proxy is back; reconnected.');
        }
        $this->logger->info('§bClient '.$this->getClientName().' has connected!');
        $event = new ClientConnectedEvent($this, $this->loader);
        $event->call();

        if ($this->session !== null) {
            $this->session->setLogInputLevel($this->loader->getLogLevel());
            $this->session->setLogOutputLevel($this->loader->getLogLevel());
        }
    }

    public function onSessionAuthenticated(): void
    {
        $this->wasConnected = true;
        $event = new ClientAuthenticatedEvent($this, $this->loader);
        $event->call();
        if ($this->session !== null && $event->isCancelled()) {
            $this->session->disconnect($event->getCancelMessage());
        }
    }

    /** Fired once per lost connection, whether the proxy said goodbye or simply vanished. */
    public function onSessionDisconnected(): void
    {
        if (!$this->wasConnected) {
            return;
        }
        $this->wasConnected = false;
        $event = new ClientDisconnectedEvent($this, $this->loader);
        $event->call();
    }

    /**
     * @param StarGatePacket $packet
     */
    public function sendPacket(StarGatePacket $packet): void
    {
        if ($this->session !== null) {
            $this->session->sendPacket($packet);
        }
    }

    /**
     * @param StarGatePacket $packet
     * @return PacketResponse|null
     */
    public function responsePacket(StarGatePacket $packet): ?PacketResponse
    {
        if ($this->session !== null) {
            return $this->session->responsePacket($packet);
        }
        return null;
    }

    public function shutdown(): void
    {
        $this->stopping = true;
        if ($this->session !== null && $this->isConnected()) {
            $this->session->disconnect(DisconnectPacket::CLIENT_SHUTDOWN);
        }
    }

    /**
     * @return bool
     */
    public function isConnected(): bool
    {
        return $this->session !== null && $this->session->isConnected();
    }

    /**
     * @return bool
     */
    public function isAuthenticated(): bool
    {
        return $this->session !== null && $this->session->isAuthenticated();
    }

    /**
     * @return HandshakeData
     */
    public function getHandshakeData(): HandshakeData
    {
        return $this->handshakeData;
    }

    /**
     * @return string
     */
    public function getClientName(): string
    {
        return $this->handshakeData->getClientName();
    }

    /**
     * @return ClientSession|null
     */
    public function getSession(): ?ClientSession
    {
        return $this->session;
    }

    /**
     * @return Server
     */
    public function getServer(): Server
    {
        return $this->server;
    }

    public function getLogger(): \AttachableLogger
    {
        return $this->logger;
    }

    /**
     * @return ProtocolCodec
     */
    public function getProtocolCodec(): ProtocolCodec
    {
        return $this->protocolCodec;
    }

    /**
     * @return SessionHandler|null
     */
    public function getCustomHandler(): ?SessionHandler
    {
        return $this->customHandler;
    }

    /**
     * @param SessionHandler $customHandler
     */
    public function setCustomHandler(SessionHandler $customHandler): void
    {
        $this->customHandler = $customHandler;
    }
}
