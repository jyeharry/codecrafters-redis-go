package main

import "time"

type Database struct {
	db     map[string]string
	expiry map[string]time.Time
}

func NewDatabase() *Database {
	return &Database{
		db:     make(map[string]string),
		expiry: make(map[string]time.Time),
	}
}

func (database *Database) Set(key string, value string, expiry uint) {
	database.db[key] = value
	if expiry > 0 {
		database.expiry[key] = time.Now().Add(time.Duration(expiry) * time.Millisecond)
	}
}

func (database *Database) Get(key string) (string, bool) {
	if expiry, ok := database.expiry[key]; ok && time.Now().After(expiry) {
		delete(database.db, key)
		delete(database.expiry, key)
		return "", false
	}
	val, ok := database.db[key]
	if !ok {
		return "", false
	}
	return val, true
}
