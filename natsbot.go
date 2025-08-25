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
	"log"
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

		case <-timer.C:
		}

		runTimers(L, timer)

		if tableIsEmpty(stateConnTable(L)) && tableIsEmpty(stateTimerTable(L)) {
			break
		}
	}

	log.Println("natsbot finished")
}

func processMsg(L *lua.LState, msg *nats.Msg) {
	// TODO
}

/********** State Object in the Lua Interpreter **********/

const luaStateName = "_natsbot"
const (
	_ = iota
	keyMsgChan
	keyCfgMap
	keyConnTable
	keyTimerTable
)

func registerState(L *lua.LState, msgChan chan<- *nats.Msg) {
	st := L.NewTable()
	L.RawSetInt(st, keyMsgChan, newUserData(L, msgChan))
	L.RawSetInt(st, keyCfgMap, newUserData(L, make(natsConfigMap)))
	L.RawSetInt(st, keyConnTable, L.NewTable())
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

func stateMsgChan(L *lua.LState) chan<- *nats.Msg {
	ud := stateValue(L, keyMsgChan)
	return ud.(*lua.LUserData).Value.(chan<- *nats.Msg)
}

func stateCfgMap(L *lua.LState) natsConfigMap {
	return stateValue(L, keyCfgMap).(*lua.LUserData).Value.(natsConfigMap)
}

func stateConnTable(L *lua.LState) *lua.LTable {
	return stateValue(L, keyConnTable).(*lua.LTable)
}

func stateTimerTable(L *lua.LState) *lua.LTable {
	return stateValue(L, keyTimerTable).(*lua.LTable)
}

/********** NATS Connection Configuration **********/

type natsConfig struct {
	url string
}

type natsConn struct {
	nc *nats.Conn
	id int
}

type natsConfigMap map[natsConfig]natsConn

/********** Lua Object for NATS connection **********/

func registerConnType(L *lua.LState) {
	// TODO
}

func cleanupConns(L *lua.LState) {
	// TODO
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

func newUserData(L *lua.LState, v interface{}) *lua.LUserData {
	res := L.NewUserData()
	res.Value = v
	return res
}

func tableIsEmpty(t *lua.LTable) bool {
	key, _ := t.Next(lua.LNil)
	return key == lua.LNil
}
