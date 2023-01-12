# flowDB

flowDB is a JSON database built in a MongoDB-esque fashion -- it aims to replicate some of the core functionalities of a MongoDB-like database.

This was a project I worked on towards the beginning of my journey learning the Go programming language.

flowDB has a database driver, which provides an interface between the Go code and the actual database filesystem. This driver provides support for the following functions:
* `CreateDB`
* `Write`
* `Read`
* `ReadAll`
* `Delete`
* `DeleteAll`

Within the `pkg` directory, there is an `example` subdirectory with a file `example.go`. This file contains code with some brief examples that demonstrate how each of the core database functions listed above can be used. _NOTE_: The code in this file is also the code that is run by the `main.go` file.
