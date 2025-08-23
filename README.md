# NATS bot

[![Casual Maintenance Intended](https://casuallymaintained.tech/badge.svg)](https://casuallymaintained.tech/)

This is a daemon [NATS](https://nats.io/) client which
runs Lua callbacks on received messages.

It is inspired by [an earlier MQTT bot](https://fossil.instinctive.eu/mqttagent/),
meant to be a kind of cron-but-for-MQTT-messages-instead-of-time to run actions,
but ported to NATS instead of MQTT.
