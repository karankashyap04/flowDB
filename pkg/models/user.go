package models

import "encoding/json"

type User struct {
	Name string
	Age json.Number
	Occupation string
	Contact string
	Address Address
}