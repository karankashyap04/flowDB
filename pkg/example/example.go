package example

import (
	"encoding/json"
	driver "flowDB/pkg/driver"
	models "flowDB/pkg/models"
	"fmt"
)

// creating 2 sample users, one with keyed fields and one with unkeyed fields -- flowDB works in both cases
var sampleUser1 models.User = models.User{Name: "User 1", Age: "19", Occupation: "SWE", Email: "user1@gmail.com", Address: models.Address{Home: "House 1", City: "Providence", State: "Rhode Island", Country: "USA"}}
var sampleUser2 models.User = models.User{"User 2", "21", "IB", "user2@company.com",
models.Address{"House 2", "NYC", "New York", "USA"}}

func RunExample() {
	fmt.Println("flowDB -- built with GoLang!")

	dbDir := "./db/"
	db, err := driver.CreateDB(dbDir)
	checkError(err)

	// sample data
	people := []models.User{sampleUser1, sampleUser2}

	for _, value := range people {
		fmt.Println("Trying to write a value!")
		db.Write("users", value.Name, value)
	}

	var readUser models.User
	db.Read("users", "User 1", &readUser)
	fmt.Println(readUser)

	dbEntries, err := db.ReadAll("users")
	checkError(err)
	fmt.Println(dbEntries)

	allPeople := make([]models.User, 0)
	for _, entry := range dbEntries {
		var person models.User
		err := json.Unmarshal([]byte(entry), &person)
		checkError(err)
		allPeople = append(allPeople, person)
	}
	fmt.Println(allPeople)

	err = db.Delete("users", "User 1")
	checkError(err)

	// err = db.DeleteAll("users", "")
	// checkError(err)
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}