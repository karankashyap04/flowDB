package models

import "encoding/json"

type Address struct {
	Home string
	City string
	State string
	Country string
	Pincode json.Number
}