package main

import "strings"

func strIsEmpty(str string) bool {
	return len(strings.TrimSpace(str)) == 0
}
