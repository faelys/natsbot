/*
 * Copyright (c) 2025, Natacha Porté
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package natsbot

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/yuin/gopher-lua"
)

type NatsBot interface {
	Setup(L *lua.LState)
	Teardown(L *lua.LState)
}

func Loop(cb NatsBot, mainScript string, capacity int) {
	msgChan := make(chan *nats.Msg, capacity)
	toClean := make(map[*nats.Subscription]bool)

	L := lua.NewState()
	defer L.Close()

	cb.Setup(L)
	defer cb.Teardown(L)

	registerConnType(L)
	registerTimerType(L)
	registerState(L, msgChan)

	if err := L.DoFile(mainScript); err != nil {
		panic(err)
	}

	timer := time.NewTimer(0)
	defer timer.Stop()

	log.Println("natsbot started")

	for {
		select {
		case msg, ok := <-msgChan:

			if !ok {
				log.Println("msgChan is closed")
				break
			}

			processMsg(L, msg)

			if !msg.Sub.IsValid() {
				toClean[msg.Sub] = true
			}

		case <-timer.C:
		}

		runTimers(L, timer)

		if len(msgChan) == 0 {
			for s := range toClean {
				if !s.IsValid() {
					log.Printf("Pruning subscription %q", s.Subject)
					tbl, idx := stateSubsTable(L)
					L.RawSetInt(tbl, idx[s], lua.LNil)
					delete(idx, s)
				} else {
					log.Printf("Subscription %q is still valid", s.Subject)
				}
			}
			toClean = make(map[*nats.Subscription]bool)
		}

		if tableWithIndexIsEmpty(stateSubsTable(L)) && tableIsEmpty(stateTimerTable(L)) {
			break
		}
	}

	log.Println("natsbot finished")
}

func processMsg(L *lua.LState, msg *nats.Msg) {
	tbl, idx := stateSubsTable(L)
	subs := L.RawGetInt(tbl, idx[msg.Sub])
	fn := L.GetField(L.GetMetatable(subs), "__call")
	err := L.CallByParam(lua.P{Fn: fn, NRet: 0, Protect: true},
		subs,
		lua.LString(msg.Subject),
		lua.LString(string(msg.Data)),
		luaHeaders(L, msg.Reply, msg.Header))
	if err != nil {
		panic(err)
	}
}

/********** State Object in the Lua Interpreter **********/

const luaStateName = "_natsbot"
const (
	_ = iota
	keyMsgChan
	keyCfgMap
	keyConnTable
	keySubsTable
	keyTimerTable
)

type subsMap map[*nats.Subscription]int

func registerState(L *lua.LState, msgChan chan *nats.Msg) {
	conns := L.NewTable()
	L.RawSetInt(conns, 1, newUserData(L, make(connMap)))

	subs := L.NewTable()
	L.RawSetInt(subs, 1, newUserData(L, make(subsMap)))

	st := L.NewTable()
	L.RawSetInt(st, keyMsgChan, newUserData(L, msgChan))
	L.RawSetInt(st, keyCfgMap, newUserData(L, make(natsConfigMap)))
	L.RawSetInt(st, keyConnTable, conns)
	L.RawSetInt(st, keySubsTable, subs)
	L.RawSetInt(st, keyTimerTable, L.NewTable())
	stateSet(L, st)
}

func stateUncheckedGet(L *lua.LState) *lua.LTable {
	v := L.GetField(L.Get(lua.RegistryIndex), luaStateName)
	if result, ok := v.(*lua.LTable); ok {
		return result
	} else {
		return nil
	}
}

func stateGet(L *lua.LState) *lua.LTable {
	result := stateUncheckedGet(L)
	if result == nil {
		panic("Missing internal state object")
	}
	return result
}

func stateSet(L *lua.LState, newState *lua.LTable) {
	if stateUncheckedGet(L) != nil {
		panic("Overwriting internal state object")
	}
	L.SetField(L.Get(lua.RegistryIndex), luaStateName, newState)
}

func stateValue(L *lua.LState, key int) lua.LValue {
	return L.RawGetInt(stateGet(L), key)
}

func stateMsgChan(L *lua.LState) chan *nats.Msg {
	ud := stateValue(L, keyMsgChan)
	return ud.(*lua.LUserData).Value.(chan *nats.Msg)
}

func stateCfgMap(L *lua.LState) natsConfigMap {
	return stateValue(L, keyCfgMap).(*lua.LUserData).Value.(natsConfigMap)
}

func stateConnTable(L *lua.LState) (*lua.LTable, connMap) {
	tbl := stateValue(L, keyConnTable).(*lua.LTable)
	idx := L.RawGetInt(tbl, keyIndex).(*lua.LUserData).Value.(connMap)
	return tbl, idx
}

func stateSubsTable(L *lua.LState) (*lua.LTable, subsMap) {
	tbl := stateValue(L, keySubsTable).(*lua.LTable)
	idx := L.RawGetInt(tbl, keyIndex).(*lua.LUserData).Value.(subsMap)
	return tbl, idx
}

func stateTimerTable(L *lua.LState) *lua.LTable {
	return stateValue(L, keyTimerTable).(*lua.LTable)
}

/********** NATS Connection Configuration **********/

type natsConfig struct {
	url      string
	name     string
	nkey     string
	user     string
	password string
	token    string
}

type natsConfigMap map[natsConfig]*nats.Conn

type natsCbMap map[string]*lua.LFunction

func connConfig(L *lua.LState) (*natsConfig, natsCbMap, error) {
	arg := L.Get(1)
	if url, ok := arg.(lua.LString); ok {
		if cfg, cbmap, err := toConfig(L, L.Get(2)); err != nil {
			return nil, nil, err
		} else if cfg.url == "" {
			cfg.url = string(url)
			return cfg, cbmap, nil
		} else if cfg.url != string(url) {
			return nil, nil, fmt.Errorf("incompatible URLs %q and %q", cfg.url, string(url))
		} else {
			return cfg, cbmap, nil
		}
	} else {
		return toConfig(L, arg)
	}
}

func toConfig(L *lua.LState, lv lua.LValue) (*natsConfig, natsCbMap, error) {
	tbl, ok := lv.(*lua.LTable)
	if !ok {
		return nil, nil, errors.New("configuration is not a table")
	}

	var result natsConfig
	cbmap := make(natsCbMap)
	var errStr []string

	L.ForEach(tbl, func(key, value lua.LValue) {
		skey, ok := key.(lua.LString)
		if !ok {
			errStr = append(errStr, fmt.Sprintf("bad key: %q", lua.LVAsString(key)))
			return
		}

		switch skey {
		case "closed":
			fallthrough
		case "disconnected":
			fallthrough
		case "error":
			fallthrough
		case "reconnect_error":
			fallthrough
		case "reconnected":
			if s, ok := value.(*lua.LFunction); ok {
				cbmap[string(skey)] = s
			} else {
				errStr = append(errStr, fmt.Sprintf("bad value for key %q: %q", skey, lua.LVAsString(value)))
			}

		case "url":
			if s, ok := value.(lua.LString); ok {
				result.url = string(s)
			} else {
				errStr = append(errStr, fmt.Sprintf("bad value for key %q: %q", skey, lua.LVAsString(value)))
			}

		case "name":
			if s, ok := value.(lua.LString); ok {
				result.name = string(s)
			} else {
				errStr = append(errStr, fmt.Sprintf("bad value for key %q: %q", skey, lua.LVAsString(value)))
			}

		case "nkey":
			if s, ok := value.(lua.LString); ok {
				result.nkey = string(s)
			} else {
				errStr = append(errStr, fmt.Sprintf("bad value for key %q: %q", skey, lua.LVAsString(value)))
			}

		case "user":
			if s, ok := value.(lua.LString); ok {
				result.user = string(s)
			} else {
				errStr = append(errStr, fmt.Sprintf("bad value for key %q: %q", skey, lua.LVAsString(value)))
			}

		case "password":
			if s, ok := value.(lua.LString); ok {
				result.password = string(s)
			} else {
				errStr = append(errStr, fmt.Sprintf("bad value for key %q: %q", skey, lua.LVAsString(value)))
			}

		case "token":
			if s, ok := value.(lua.LString); ok {
				result.token = string(s)
			} else {
				errStr = append(errStr, fmt.Sprintf("bad value for key %q: %q", skey, lua.LVAsString(value)))
			}

		default:
			errStr = append(errStr, fmt.Sprintf("Unknown key %q", skey))
		}
	})

	if len(errStr) > 0 {
		return nil, nil, errors.New(strings.Join(errStr, ", "))
	} else {
		return &result, cbmap, nil
	}
}

func newConn(cfg *natsConfig) (*nats.Conn, error) {
	opt := []nats.Option{}

	if cfg.name != "" {
		opt = append(opt, nats.Name(cfg.name))
	}
	if cfg.nkey != "" {
		if o, err := nats.NkeyOptionFromSeed(cfg.nkey); err != nil {
			return nil, err
		} else {
			opt = append(opt, o)
		}
	}
	if cfg.user != "" || cfg.password != "" {
		opt = append(opt, nats.UserInfo(cfg.user, cfg.password))
	}
	if cfg.token != "" {
		opt = append(opt, nats.Token(cfg.token))
	}

	return nats.Connect(cfg.url, opt...)
}

/********** Lua Object for NATS connection **********/

const luaNatsConnTypeName = "natsconn"
const keyIndex = 1

type connMap map[*nats.Conn]int

func registerConnType(L *lua.LState) {
	L.SetGlobal(luaNatsConnTypeName, L.NewFunction(natsConnect))
}

func natsConnect(L *lua.LState) int {
	pcfg, cbmap, err := connConfig(L)
	if err != nil {
		log.Println(err)
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}
	cfg := *pcfg

	cfgMap := stateCfgMap(L)
	if nc, found := cfgMap[cfg]; found {
		tbl, idx := stateConnTable(L)
		if id, ok := idx[nc]; ok {
			res := L.RawGetInt(tbl, id)
			if lua.LVIsFalse(res) {
				panic("Inconsistent connection table")
			}
			L.Push(res)
			return 1
		} else {
			panic("Inconsistent connection table")
		}
	}

	nc, err := newConn(pcfg)
	if err != nil {
		log.Println("newConn", err)
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}

	cfgMap[cfg] = nc
	L.Push(wrapConn(L, nc, cbmap))
	return 1
}

func wrapConn(L *lua.LState, nc *nats.Conn, cbmap natsCbMap) lua.LValue {
	luaConn := newUserData(L, nc)

	index := L.NewTable()
	L.SetField(index, "publish", L.NewFunction(natsPublish))
	L.SetField(index, "subscribe", L.NewFunction(natsSubscribe))

	for key, fn := range cbmap {
		L.SetField(index, key, fn)
	}

	mt := L.NewTable()
	L.SetField(mt, "__index", index)
	L.SetMetatable(luaConn, mt)

	tbl, idx := stateConnTable(L)
	id := tbl.Len() + 1
	L.RawSetInt(tbl, id, luaConn)

	if _, found := idx[nc]; found {
		panic("id already in connection table")
	}
	idx[nc] = id

	return luaConn
}

func checkConn(L *lua.LState, index int) *nats.Conn {
	ud := L.CheckUserData(index)

	if v, ok := ud.Value.(*natsSubs); ok {
		return v.nc
	}

	if v, ok := ud.Value.(*nats.Conn); ok {
		return v
	}

	L.ArgError(index, "connection expected")
	return nil
}

func natsPublish(L *lua.LState) int {
	nc := checkConn(L, 1)
	subject := L.CheckString(2)
	data := L.OptString(3, "")

	if err := nc.Publish(subject, []byte(data)); err != nil {
		log.Println("Publish:", err)
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	} else {
		L.Push(lua.LTrue)
		return 1
	}
}

func natsSubscribe(L *lua.LState) int {
	nc := checkConn(L, 1)
	subject := L.CheckString(2)
	fn := L.CheckFunction(3)

	if s, err := nc.ChanSubscribe(subject, stateMsgChan(L)); err != nil {
		log.Println("Subscribe:", err)
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	} else {
		L.Push(wrapSubs(L, fn, s, nc))
		return 1
	}
}

/********** Lua Object for NATS subscription **********/

type natsSubs struct {
	id   int
	nc   *nats.Conn
	subs *nats.Subscription
}

func wrapSubs(L *lua.LState, fn lua.LValue, ns *nats.Subscription, nc *nats.Conn) lua.LValue {
	tbl, subsIdx := stateSubsTable(L)
	id := tbl.Len() + 1
	luaSub := newUserData(L, &natsSubs{id: id, nc: nc, subs: ns})

	subsIdx[ns] = id
	L.RawSetInt(tbl, id, luaSub)

	index := L.NewTable()
	L.SetField(index, "callback", fn)
	L.SetField(index, "id", lua.LNumber(id))
	L.SetField(index, "subject", lua.LString(string(ns.Subject)))

	L.SetField(index, "publish", L.NewFunction(natsPublish))
	L.SetField(index, "subscribe", L.NewFunction(natsSubscribe))
	L.SetField(index, "unsubscribe", L.NewFunction(natsUnsubscribe))

	mt := L.NewTable()
	L.SetField(mt, "__call", fn)
	L.SetField(mt, "__index", index)
	L.SetField(mt, "__newindex", L.NewFunction(subsNewIndex))

	L.SetMetatable(luaSub, mt)
	return luaSub
}

func subsNewIndex(L *lua.LState) int {
	_ = checkSubs(L, 1)
	mt := L.GetMetatable(L.Get(1))
	key := L.Get(2)

	if s, ok := key.(lua.LString); !ok || string(s) != "callback" {
		L.RaiseError("attempt to change bad subscription field")
		return 0
	}

	fn := L.CheckFunction(3)
	L.SetField(mt, "__call", fn)
	index := L.GetField(mt, "__index").(*lua.LTable)
	L.SetField(index, "callback", fn)
	return 0
}

func checkSubs(L *lua.LState, index int) *natsSubs {
	ud := L.CheckUserData(index)

	if v, ok := ud.Value.(*natsSubs); ok {
		return v
	}

	L.ArgError(index, "subscription expected")
	return nil
}

func natsUnsubscribe(L *lua.LState) int {
	s := checkSubs(L, 1)
	count := L.OptInt(2, 0)

	if err := s.subs.AutoUnsubscribe(count); err != nil {
		log.Println("Unsubscribe:", err)
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	} else {
		L.Push(lua.LTrue)
		return 1
	}
}

/********** Lua Object for timers **********/

const luaTimerTypeName = "timer"

func registerTimerType(L *lua.LState) {
	mt := L.NewTypeMetatable(luaTimerTypeName)
	L.SetGlobal(luaTimerTypeName, mt)
	L.SetField(mt, "new", L.NewFunction(newTimer))
	L.SetField(mt, "schedule", L.NewFunction(timerSchedule))
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), timerMethods))
}

func newTimer(L *lua.LState) int {
	atTime := L.Get(1)
	cb := L.CheckFunction(2)
	L.Pop(2)
	L.SetMetatable(cb, L.GetTypeMetatable(luaTimerTypeName))
	L.Push(cb)
	L.Push(atTime)
	return timerSchedule(L)
}

var timerMethods = map[string]lua.LGFunction{
	"cancel":   timerCancel,
	"schedule": timerSchedule,
}

func timerCancel(L *lua.LState) int {
	timer := L.CheckFunction(1)
	L.RawSet(stateTimerTable(L), timer, lua.LNil)
	return 0
}

func timerSchedule(L *lua.LState) int {
	timer := L.CheckFunction(1)
	atTime := lua.LNil
	if L.Get(2) != lua.LNil {
		atTime = L.CheckNumber(2)
	}

	L.RawSet(stateTimerTable(L), timer, atTime)
	return 0
}

func toTime(lsec lua.LNumber) time.Time {
	fsec := float64(lsec)
	sec := int64(fsec)
	nsec := int64((fsec - float64(sec)) * 1.0e9)

	return time.Unix(sec, nsec)
}

func runTimers(L *lua.LState, parentTimer *time.Timer) {
	hasNext := false
	var nextTime time.Time

	now := time.Now()
	timers := stateTimerTable(L)

	timer, luaT := timers.Next(lua.LNil)
	for timer != lua.LNil {
		t := toTime(luaT.(lua.LNumber))
		if t.Compare(now) <= 0 {
			L.RawSet(timers, timer, lua.LNil)
			err := L.CallByParam(lua.P{Fn: timer, NRet: 0, Protect: true}, timer, luaT)
			if err != nil {
				panic(err)
			}
			timer = lua.LNil
			hasNext = false
		} else if !hasNext || t.Compare(nextTime) < 0 {
			hasNext = true
			nextTime = t
		}

		timer, luaT = timers.Next(timer)
	}

	if hasNext {
		parentTimer.Reset(time.Until(nextTime))
	} else {
		parentTimer.Stop()
	}
}

/********** Tools **********/

func luaHeader(L *lua.LState, header []string) lua.LValue {
	switch len(header) {
	case 0:
		return lua.LNil
	case 1:
		return lua.LString(header[0])
	default:
		result := L.CreateTable(len(header), 0)
		for i, v := range header {
			L.RawSetInt(result, i+1, lua.LString(v))
		}
		return result
	}
}

func luaHeaders(L *lua.LState, reply string, headers map[string][]string) lua.LValue {
	result := L.NewTable()

	if reply != "" {
		L.RawSetInt(result, 1, lua.LString(reply))
	}

	for key, values := range headers {
		L.SetField(result, key, luaHeader(L, values))
	}

	return result
}

func newUserData(L *lua.LState, v interface{}) *lua.LUserData {
	res := L.NewUserData()
	res.Value = v
	return res
}

func tableIsEmpty(t *lua.LTable) bool {
	key, _ := t.Next(lua.LNil)
	return key == lua.LNil
}

func tableWithIndexIsEmpty(t *lua.LTable, idx interface{}) bool {
	key, _ := t.Next(lua.LNil)

	if n, ok := key.(lua.LNumber); ok && n == lua.LNumber(keyIndex) {
		key, _ = t.Next(key)
	}

	return key == lua.LNil
}
